"""核心业务逻辑：登记 / 核验占用 / 查询 / 撤销 / 改期 / 授权退役改挂 / 额度预留 / 候补。

并发安全
========
所有通道写事务遵守统一的全局加锁顺序，杜绝跨事务死锁：

    **通道咨询锁（region+channel）→ 到期预留结算 → 候补队列处理 →
    预留/申请/候补行 → 授权行（id 升序）→ 明细行**

通道咨询锁按 ``region+channel`` 分桶：同一通道内的提交、预留、确认、
取消、撤销、改期、改挂、候补受理与候补成交全部串行判定。授权行只可能被
同通道事务竞争（授权匹配维度即 region+channel，改挂也要求两端通道一致），
因此不同通道的事务不会争用同一授权行，跨桶不可能形成等待环；同事务内对
同一批授权一律按 id 升序 ``SELECT ... FOR UPDATE``。

到期释放（惰性结算）
====================
预留不依赖后台定时任务：任一通道写事务在咨询锁内、业务判定之前，首先
把该通道 ``expires_at <= now`` 的待确认预留原子标记为 ``expired`` 并
逐项归还 ``reserved_count``。因此预留一旦到期，对随后的提交/预留/余量
判定立即可见（「到期即释放」）。预留查询（单条）同样先做通道结算，
故逾期确认/取消必被拒绝。

候补队列
========
候补申请沿用既有匹配与重叠授权选择规则：全部可承接则直接成交（生成
approved 申请）；每项均有匹配授权但额度不足才入本通道队列（不占额）；
任一项缺少匹配授权则整体逐项拒绝。撤销、预留取消/到期、改期、改挂在
通道锁内释放额度后，以及新直接申请/新候补受理前，都会按受理顺序重算
队首：可整体承接则原子占额转为一笔 approved 申请并继续下一队首；
额度仍不足则停止本轮；授权已不再匹配则标记失败并继续后项。因此新直接
申请永远不会抢走可成交候补的额度。

额度模型
========
- ``used_count``：已通过（未撤销）发行申请的正式占用，语义与历史版本
  完全一致，``remaining = max_count - used_count`` 保持兼容。
- ``reserved_count``：未到期待确认预留占用；确认时逐项转为正式占用
  （reserved-1、used+1），取消/到期时逐项归还。
- 不变量：``used_count >= 0``、``reserved_count >= 0``、
  ``used_count + reserved_count <= max_count``。

所有扣减/归还均采用带条件的 UPDATE 并断言影响行数，作为绝不超额、
绝不重复释放的最后防线。
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
from zlib import crc32

from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app import models
from app.errors import (
    AlreadyRevoked,
    DistributionRejected,
    DistributionRevoked,
    DuplicateMaterialItems,
    InvalidReservationStatus,
    InvalidWaitlistStatus,
    MigrationRejected,
    MaterialCodeTaken,
    RescheduleRejected,
    ReservationAlreadyCancelled,
    ReservationAlreadyConfirmed,
    ReservationCancelled,
    ReservationExpired,
    ReservationRejected,
    ResourceNotFound,
    UnknownMaterial,
    WaitlistAlreadyCancelled,
    WaitlistAlreadyFailed,
    WaitlistAlreadyFulfilled,
    WaitlistNotPending,
    WaitlistRejected,
)
from app.schemas import (
    AuthorizationCreate,
    AuthorizationMigrationRequest,
    DistributionCreate,
    DistributionReschedule,
    MaterialCreate,
    ReservationCreate,
    WaitlistCreate,
)


# ---------------------------------------------------------------- 登记素材
def create_material(db: Session, data: MaterialCreate) -> models.Material:
    material = models.Material(code=data.code, name=data.name)
    db.add(material)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        raise MaterialCodeTaken(data.code)
    db.commit()
    db.refresh(material)
    return material


def get_material(db: Session, material_id: int) -> models.Material:
    material = db.get(models.Material, material_id)
    if material is None:
        raise ResourceNotFound("素材", material_id)
    return material


# ------------------------------------------------------------- 登记授权
def create_authorization(
    db: Session, data: AuthorizationCreate
) -> models.Authorization:
    if db.get(models.Material, data.material_id) is None:
        raise UnknownMaterial([data.material_id])

    auth = models.Authorization(
        material_id=data.material_id,
        region=data.region,
        channel=data.channel,
        starts_at=data.starts_at,
        ends_at=data.ends_at,
        max_count=data.max_count,
    )
    db.add(auth)
    db.commit()
    db.refresh(auth)
    return auth


def get_authorization(db: Session, authorization_id: int) -> models.Authorization:
    """查询授权（含已用/剩余/预留）。

    查询前在该授权所在通道内做一次到期结算并锁定授权行，因此返回的
    ``reserved_count`` / ``reservable_remaining`` 是扣除已到期预留后的
    准确值；``used_count``/``remaining`` 的正式占用语义不变。
    """
    auth = db.get(models.Authorization, authorization_id)
    if auth is None:
        raise ResourceNotFound("授权", authorization_id)
    try:
        _channel_lock(db, auth.region, auth.channel)
        now = utcnow()
        _settle_expired(
            db,
            now,
            region=auth.region,
            channel=auth.channel,
            extra_auth_ids=[authorization_id],
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    db.refresh(auth)
    return auth


def set_authorization_status(
    db: Session, authorization_id: int, status: str
) -> models.Authorization:
    auth = db.get(models.Authorization, authorization_id)
    if auth is None:
        raise ResourceNotFound("授权", authorization_id)
    auth.status = status
    db.commit()
    db.refresh(auth)
    return auth


# ----------------------------------------------------------- 并发基础设施
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _bucket_lock_id(region: str, channel: str) -> int:
    """region+channel → 单个有符号 32 位整数（pg_advisory_xact_lock 参数）。"""
    raw = crc32(f"{region}\x1f{channel}".encode("utf-8"))
    return raw - 0x100000000 if raw >= 0x80000000 else raw


def _channel_lock(db: Session, region: str, channel: str) -> None:
    """第一加锁点：通道事务级咨询锁（同事务可重入，提交/回滚自动释放）。"""
    db.execute(
        text("SELECT pg_advisory_xact_lock(:lk)"),
        {"lk": _bucket_lock_id(region, channel)},
    )


def _validate_material_ids(db: Session, material_ids: list[int]) -> None:
    """重复素材 / 未知编号校验（与申请、预留共用）。"""
    counts = Counter(material_ids)
    duplicated = sorted(mid for mid, n in counts.items() if n > 1)
    if duplicated:
        raise DuplicateMaterialItems(duplicated)

    found_ids = set(
        db.scalars(
            select(models.Material.id).where(
                models.Material.id.in_(material_ids)
            )
        )
    )
    unknown = [mid for mid in material_ids if mid not in found_ids]
    if unknown:
        raise UnknownMaterial(sorted(unknown))


def _lock_authorizations(
    db: Session, auth_ids: list[int]
) -> list[models.Authorization]:
    """按 id 升序行锁授权并以最新行值刷新（调用方须已持有通道咨询锁）。

    ``populate_existing``：调用方若在取锁前已不加锁读过同一授权行，
    此处用锁定后的最新行值刷新其属性，保证随后的余量判定基于行锁
    保护下的真实计数。
    """
    if not auth_ids:
        return []
    return list(
        db.scalars(
            select(models.Authorization)
            .where(models.Authorization.id.in_(auth_ids))
            .order_by(models.Authorization.id.asc())
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )


def _settle_expired(
    db: Session,
    now: datetime,
    region: str | None = None,
    channel: str | None = None,
    extra_auth_ids: list[int] | None = None,
) -> list[models.Authorization]:
    """在已持有通道咨询锁的前提下，结算到期未确认预留并返回锁定的授权行。

    - 锁定本通道（或全局，列表场景不调用本函数）所有 ``expires_at <= now``
      的待确认预留行（FOR UPDATE）；
    - 把这些预留涉及的授权 id 与 ``extra_auth_ids``（业务候选授权）合并，
      按 id 升序一次性 ``FOR UPDATE`` 锁定并读取最新行；
    - 逐项条件归还 ``reserved_count``，预留置为 expired；
    - 到期释放额度后按受理顺序重算本通道候补队首（成交会消耗刚释放的
      额度），随后重新锁定并返回最新授权行；
    - 调用方随后在同一事务内基于返回的授权行做余量判定。
    """
    resv_stmt = (
        select(models.Reservation)
        .where(
            models.Reservation.status == "pending",
            models.Reservation.expires_at <= now,
        )
        .order_by(models.Reservation.id.asc())
        .with_for_update()
    )
    if region is not None:
        resv_stmt = resv_stmt.where(
            models.Reservation.region == region,
            models.Reservation.channel == channel,
        )
    expiring = list(db.scalars(resv_stmt))

    auth_counts: Counter[int] = Counter()
    for resv in expiring:
        items = list(
            db.scalars(
                select(models.ReservationItem)
                .where(
                    models.ReservationItem.reservation_id == resv.id
                )
                .order_by(models.ReservationItem.id.asc())
            )
        )
        auth_counts.update(item.authorization_id for item in items)

    lock_ids = sorted(set(auth_counts) | set(extra_auth_ids or []))
    locked = _lock_authorizations(db, lock_ids)

    # 条件归还：每个授权按到期预留条数一次性扣减 reserved_count。
    for auth_id, n in auth_counts.items():
        result = db.execute(
            update(models.Authorization)
            .where(
                models.Authorization.id == auth_id,
                models.Authorization.reserved_count >= n,
            )
            .values(
                reserved_count=models.Authorization.reserved_count - n
            )
        )
        if result.rowcount != 1:
            db.rollback()
            raise RuntimeError(
                f"到期释放失败：授权 {auth_id} reserved_count 不足 {n}"
            )

    if expiring:
        db.execute(
            update(models.Reservation)
            .where(
                models.Reservation.id.in_([r.id for r in expiring]),
                models.Reservation.status == "pending",
            )
            .values(status="expired", expired_at=now)
        )
        db.flush()
        # 上面的 Core UPDATE 绕过了 ORM；使本事务后续属性读取拿到新值。
        db.expire_all()
        if region is not None:
            # 到期释放额度后，先重算本通道候补队首（成交会消耗刚释放的
            # 额度，失败标记会推进队列）。
            _process_waitlist(db, region, channel)
        # 候补成交可能消耗了本批授权额度：重新锁定读取最新计数后返回。
        locked = _lock_authorizations(db, lock_ids)
    return locked


def _evaluate(
    material_ids: list[int],
    locked_rows: list[models.Authorization],
    *,
    region: str,
    channel: str,
    occur: datetime,
    for_reservation: bool,
    used_discount: dict[int, int] | None = None,
) -> tuple[dict[int, models.Authorization], list[dict]]:
    """沿用既有授权匹配与重叠选择顺序做逐项判定。

    选择顺序：匹配（素材/地区/渠道/左闭右开时段）→ 仅启用 →
    ``(ends_at 升序, id 升序)`` → 第一条有余量者。余量对正式申请按
    ``used + reserved < max``（未到期预留占额），预留申请同理（预留同样
    不得超额）。``used_discount``（授权 id → 扣减次数）用于改期场景：
    判定余量时先扣除本申请在该授权上的原占用，未到期预留仍全额计入。
    返回 ``(逐项命中授权, 失败原因列表)``。
    """
    discount = used_discount or {}
    grouped: dict[int, list[models.Authorization]] = {
        mid: [] for mid in material_ids
    }
    for auth in locked_rows:
        if (
            auth.region == region
            and auth.channel == channel
            and auth.starts_at <= occur < auth.ends_at
            and auth.material_id in grouped
        ):
            grouped[auth.material_id].append(auth)

    reasons: list[dict] = []
    chosen: dict[int, models.Authorization] = {}
    for index, mid in enumerate(material_ids):
        candidates = sorted(grouped[mid], key=lambda a: (a.ends_at, a.id))
        if not candidates:
            reasons.append(
                {
                    "item_index": index,
                    "material_id": mid,
                    "reason": "no_matching_authorization",
                    "message": "不存在同时匹配地区、渠道与发行时刻的授权",
                }
            )
            continue

        active = [a for a in candidates if a.status == "active"]
        if not active:
            reasons.append(
                {
                    "item_index": index,
                    "material_id": mid,
                    "reason": "authorization_inactive",
                    "message": "匹配的授权均已停用（停用仅影响新申请）",
                }
            )
            continue

        with_quota = next(
            (
                a
                for a in active
                if a.used_count - discount.get(a.id, 0) + a.reserved_count
                < a.max_count
            ),
            None,
        )
        if with_quota is None:
            detail = active[0]
            if for_reservation:
                message = (
                    f"匹配授权（id={detail.id}）可预留额度已用尽："
                    f"正式占用 {detail.used_count}、未到期预留 "
                    f"{detail.reserved_count}/{detail.max_count}"
                )
            else:
                message = (
                    f"匹配授权（id={detail.id}）可发行次数已用尽："
                    f"{detail.used_count}/{detail.max_count}"
                    + (
                        f"（另有未到期预留 {detail.reserved_count}）"
                        if detail.reserved_count
                        else ""
                    )
                )
            reasons.append(
                {
                    "item_index": index,
                    "material_id": mid,
                    "reason": "quota_exhausted",
                    "message": message,
                }
            )
            continue
        chosen[mid] = with_quota
    return chosen, reasons


def _candidate_auth_ids(
    db: Session,
    material_ids: list[int],
    region: str,
    channel: str,
    occur: datetime,
) -> list[int]:
    """不加锁预读候选授权 id（用于合并统一的 id 升序行锁集合）。"""
    return list(
        db.scalars(
            select(models.Authorization.id)
            .where(
                models.Authorization.region == region,
                models.Authorization.channel == channel,
                # 左闭右开：[starts_at, ends_at)
                models.Authorization.starts_at <= occur,
                models.Authorization.ends_at > occur,
                models.Authorization.material_id.in_(material_ids),
            )
        )
    )


# ------------------------------------------------------------- 候补队列
def _process_waitlist(db: Session, region: str, channel: str) -> int:
    """在通道咨询锁内按受理顺序处理本通道候补队列，返回本轮成交笔数。

    每轮只看队首（``id`` 最小的 pending 候补）：

    - 队首**可整体承接**：原子占额并转为一笔 approved 申请
      （``fulfilled``），继续处理下一队首；
    - 队首**授权已不再匹配**（任一项无匹配授权或匹配授权均已停用）：
      标记 ``failed`` 并记录失败原因，继续处理后续候补；
    - 队首**仍匹配但额度不足**：停止本轮，队首留在队列等待下次释放
      （严格按受理顺序，不跳过队首看后续候补）。
    """
    fulfilled = 0
    while True:
        head = db.scalar(
            select(models.Waitlist)
            .where(
                models.Waitlist.region == region,
                models.Waitlist.channel == channel,
                models.Waitlist.status == "pending",
            )
            .order_by(models.Waitlist.id.asc())
            .limit(1)
            .with_for_update()
        )
        if head is None:
            return fulfilled

        material_ids = list(
            db.scalars(
                select(models.WaitlistItem.material_id)
                .where(models.WaitlistItem.waitlist_id == head.id)
                .order_by(models.WaitlistItem.item_index.asc())
            )
        )
        candidate_ids = _candidate_auth_ids(
            db, material_ids, region, channel, head.occur_at
        )
        locked_rows = _lock_authorizations(db, candidate_ids)
        chosen, reasons = _evaluate(
            material_ids,
            locked_rows,
            region=region,
            channel=channel,
            occur=head.occur_at,
            for_reservation=False,
        )

        if reasons:
            blocking = next(
                (r for r in reasons if r["reason"] != "quota_exhausted"),
                None,
            )
            if blocking is not None:
                # 授权已不再匹配：终态失败，继续处理后续候补。
                head.status = "failed"
                head.failure_reason = blocking["reason"]
                head.failed_at = utcnow()
                db.flush()
                continue
            # 额度仍不足：停止本轮。
            return fulfilled

        # 整体可承接：条件占额（护栏断言），生成一笔 approved 申请。
        for auth in chosen.values():
            result = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == auth.id,
                    models.Authorization.used_count
                    + models.Authorization.reserved_count
                    < models.Authorization.max_count,
                )
                .values(used_count=models.Authorization.used_count + 1)
            )
            if result.rowcount != 1:
                db.rollback()
                raise RuntimeError(
                    f"候补成交失败：授权 {auth.id} 余量不足"
                )

        distribution = models.Distribution(
            region=region,
            channel=channel,
            occur_at=head.occur_at,
            status="approved",
        )
        db.add(distribution)
        db.flush()
        for index, mid in enumerate(material_ids):
            db.add(
                models.DistributionItem(
                    distribution_id=distribution.id,
                    material_id=mid,
                    authorization_id=chosen[mid].id,
                    item_index=index,
                )
            )
        head.status = "fulfilled"
        head.distribution_id = distribution.id
        head.fulfilled_at = utcnow()
        db.flush()
        fulfilled += 1


# ------------------------------------------------------- 发行核验与占用
def create_distribution(
    db: Session, data: DistributionCreate
) -> models.Distribution:
    # 1) 重复素材 / 未知编号（与预留同一套校验）。
    _validate_material_ids(db, list(data.material_ids))
    material_ids = list(data.material_ids)

    try:
        occur = data.occur_at
        # 2) 通道咨询锁：同 region+channel 的提交/预留/确认/取消/撤销/改挂
        #    全部串行判定，第一加锁点，杜绝跨事务死锁。
        _channel_lock(db, data.region, data.channel)
        now = utcnow()

        # 3) 预读候选授权 → 与本通道到期预留涉及授权合并后按 id 升序锁定，
        #    同时完成到期结算（到期预留立即释放、参与本次余量判定）。
        candidate_ids = _candidate_auth_ids(
            db, material_ids, data.region, data.channel, occur
        )
        locked_rows = _settle_expired(
            db,
            now,
            region=data.region,
            channel=data.channel,
            extra_auth_ids=candidate_ids,
        )

        # 3b) 新直接申请须先处理可成交候补，不得抢走其额度：按受理顺序
        #     重算队首后再做本次判定；候补成交消耗了额度时重读最新计数。
        if _process_waitlist(db, data.region, data.channel):
            locked_rows = _lock_authorizations(db, candidate_ids)

        # 4) 逐项判定（未到期预留参与余量：used + reserved < max）。
        chosen, reasons = _evaluate(
            material_ids,
            locked_rows,
            region=data.region,
            channel=data.channel,
            occur=occur,
            for_reservation=False,
        )
        if reasons:
            db.rollback()
            raise DistributionRejected(reasons)

        # 5) 条件占用正式次数（含预留占用护栏），断言影响行数。
        for auth in chosen.values():
            result = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == auth.id,
                    models.Authorization.used_count
                    + models.Authorization.reserved_count
                    < models.Authorization.max_count,
                )
                .values(used_count=models.Authorization.used_count + 1)
            )
            if result.rowcount != 1:
                db.rollback()
                raise DistributionRejected(
                    [
                        {
                            "material_id": auth.material_id,
                            "reason": "quota_exhausted",
                            "message": (
                                f"授权 id={auth.id} 余量不足，"
                                "并发占用冲突，本次申请整体拒绝"
                            ),
                        }
                    ]
                )

        # 6) 落库申请及逐项占用记录。
        distribution = models.Distribution(
            region=data.region,
            channel=data.channel,
            occur_at=data.occur_at,
            status="approved",
        )
        db.add(distribution)
        db.flush()
        for index, mid in enumerate(material_ids):
            db.add(
                models.DistributionItem(
                    distribution_id=distribution.id,
                    material_id=mid,
                    authorization_id=chosen[mid].id,
                    item_index=index,
                )
            )
        db.commit()
    except DistributionRejected:
        raise
    except Exception:
        db.rollback()
        raise

    return get_distribution(db, distribution_id=distribution.id)


# ------------------------------------------------------------- 查询/撤销
def get_distribution(db: Session, distribution_id: int) -> models.Distribution:
    distribution = db.get(
        models.Distribution,
        distribution_id,
        options=[
            selectinload(models.Distribution.items).selectinload(
                models.DistributionItem.material
            ),
            selectinload(models.Distribution.items).selectinload(
                models.DistributionItem.authorization
            ),
        ],
    )
    if distribution is None:
        raise ResourceNotFound("发行申请", distribution_id)
    return distribution


def revoke_distribution(
    db: Session, distribution_id: int
) -> models.Distribution:
    try:
        # 先读取申请的通道维度（不加锁），以便第一步即获取通道咨询锁。
        head = db.execute(
            select(
                models.Distribution.region,
                models.Distribution.channel,
                models.Distribution.status,
            ).where(models.Distribution.id == distribution_id)
        ).first()
        if head is None:
            raise ResourceNotFound("发行申请", distribution_id)
        region, channel, _status = head

        # 第一加锁点：该申请所在通道的咨询锁。与进行中的提交/预留/确认/
        # 取消/改挂事务互斥，同时杜绝跨事务死锁（一律先取咨询锁）。
        _channel_lock(db, region, channel)
        # 撤销前先结算本通道到期预留（归还 reserved_count），使通道总额度
        # 视图一致；撤销本身只释放正式占用 used_count。
        _settle_expired(db, utcnow(), region=region, channel=channel)

        # 再锁定申请行，串行化对同一申请的并发撤销，复查状态，
        # 保证后续判定基于最新已提交数据（并发双重撤销恰为一次
        # 200、一次 409）。
        distribution = db.scalar(
            select(models.Distribution)
            .where(models.Distribution.id == distribution_id)
            .with_for_update()
        )
        if distribution.status == "revoked":
            raise AlreadyRevoked(distribution_id)

        # 锁定本申请的全部占用明细行：与改挂事务的明细归属更新互斥，
        # 固定「明细 → 授权」的当前归属。
        items = list(
            db.scalars(
                select(models.DistributionItem)
                .where(
                    models.DistributionItem.distribution_id
                    == distribution_id
                )
                .order_by(models.DistributionItem.id.asc())
                .with_for_update()
            )
        )
        auth_ids = sorted({item.authorization_id for item in items})

        # 按授权 id 升序加锁，与核验/改挂/预留事务保持同一行锁顺序。
        if auth_ids:
            db.execute(
                select(models.Authorization.id)
                .where(models.Authorization.id.in_(auth_ids))
                .order_by(models.Authorization.id.asc())
                .with_for_update()
            )

        # 同一授权在本申请中至多出现一次（素材已去重），逐项释放。
        # 条件 ``used_count >= 1`` 作为不重复释放/账实相符的最后防线。
        for auth_id in auth_ids:
            result = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == auth_id,
                    models.Authorization.used_count >= 1,
                )
                .values(
                    used_count=models.Authorization.used_count - 1
                )
            )
            if result.rowcount != 1:
                db.rollback()
                raise RuntimeError(
                    f"撤销释放失败：授权 {auth_id} used_count 异常"
                )

        distribution.status = "revoked"
        distribution.revoked_at = datetime.now(timezone.utc)
        db.flush()

        # 撤销释放额度后，按受理顺序重算本通道候补队首。
        _process_waitlist(db, region, channel)
        db.commit()
    except (ResourceNotFound, AlreadyRevoked):
        raise
    except Exception:
        db.rollback()
        raise

    return get_distribution(db, distribution_id=distribution_id)


# --------------------------------------------------------- 发行申请改期
def reschedule_distribution(
    db: Session, distribution_id: int, data: DistributionReschedule
) -> models.Distribution:
    """运营改期：以新的带时区发行时刻调整一笔已通过申请的档期。

    - 地区、渠道、素材清单不变，仅更换 ``occur_at``；
    - 新时刻与当前时刻为**同一瞬间**（仅时区表示不同也算）：幂等返回
      原申请，不变更任何次数；
    - 否则按既有左闭右开匹配与重叠授权顺序（到期早优先、其次编号小）
      逐项重选授权；余量判定**扣除本申请原占用**（该授权
      ``used_count`` 先减回本申请占的 1 次），**仍计入未到期预留**，且
      目标授权必须启用；
    - 全部项可承接才原子更新申请时刻、逐项明细归属与相关授权次数
      （归属不变的授权次数不动；归属变化的「原授权 -1、新授权 +1」），
      返回更新后的申请；任一项失败整体拒绝并逐项列明原因，原记录与
      次数均不变；
    - 已撤销申请拒绝改期（409）；由确认预留生成的申请同样可改期，
      预留原始时刻与确认记录保留，预留查询关联申请展示最新数据。
    """
    # 不加锁读取通道维度（不可变），仅用于确定咨询锁桶。
    head = db.execute(
        select(
            models.Distribution.region,
            models.Distribution.channel,
        ).where(models.Distribution.id == distribution_id)
    ).first()
    if head is None:
        raise ResourceNotFound("发行申请", distribution_id)
    region, channel = head

    try:
        # 第一加锁点：通道咨询锁。与提交/预留/确认/取消/撤销/改挂互斥。
        _channel_lock(db, region, channel)

        # 锁定申请行，复查状态：已撤销申请拒绝改期（即使新时刻为同一瞬间）。
        distribution = db.scalar(
            select(models.Distribution)
            .where(models.Distribution.id == distribution_id)
            .with_for_update()
        )
        if distribution.status == "revoked":
            raise DistributionRevoked(distribution_id)

        # 同一瞬间：幂等空操作，返回原申请且不变动任何次数。
        if data.occur_at == distribution.occur_at:
            db.rollback()
            return get_distribution(db, distribution_id)

        # 锁定本申请明细行（与撤销/改挂互斥），确定素材清单与原归属授权。
        items = list(
            db.scalars(
                select(models.DistributionItem)
                .where(
                    models.DistributionItem.distribution_id
                    == distribution_id
                )
                .order_by(models.DistributionItem.item_index.asc())
                .with_for_update()
            )
        )
        material_ids = [item.material_id for item in items]
        own_auth_ids = {item.authorization_id for item in items}

        # 新时刻候选授权与原归属授权合并，随本通道到期结算一次性按 id
        # 升序行锁（到期预留立即释放并参与本次余量判定）。
        candidate_ids = _candidate_auth_ids(
            db, material_ids, region, channel, data.occur_at
        )
        locked_rows = _settle_expired(
            db,
            utcnow(),
            region=region,
            channel=channel,
            extra_auth_ids=sorted(set(candidate_ids) | own_auth_ids),
        )

        # 逐项重选：余量扣除本申请原占用，仍计入未到期预留。
        chosen, reasons = _evaluate(
            material_ids,
            locked_rows,
            region=region,
            channel=channel,
            occur=data.occur_at,
            for_reservation=False,
            used_discount={auth_id: 1 for auth_id in own_auth_ids},
        )
        if reasons:
            db.rollback()
            raise RescheduleRejected(reasons)

        # 原子搬移：归属变化的项逐条「原授权条件 -1、新授权条件 +1」，
        # 归属不变的项不动次数。素材在申请内唯一 ⇒ 每个授权在一次改期中
        # 至多作为一端出现一次，不会既加又减。
        for item in items:
            new_auth = chosen[item.material_id]
            old_auth_id = item.authorization_id
            if new_auth.id == old_auth_id:
                continue
            dec = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == old_auth_id,
                    models.Authorization.used_count >= 1,
                )
                .values(used_count=models.Authorization.used_count - 1)
            )
            if dec.rowcount != 1:
                db.rollback()
                raise RuntimeError(
                    f"改期迁出失败：授权 {old_auth_id} used_count 异常"
                )
            inc = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == new_auth.id,
                    models.Authorization.used_count
                    + models.Authorization.reserved_count
                    < models.Authorization.max_count,
                )
                .values(used_count=models.Authorization.used_count + 1)
            )
            if inc.rowcount != 1:
                db.rollback()
                raise RuntimeError(
                    f"改期迁入失败：授权 {new_auth.id} 余量不足"
                )
            item.authorization_id = new_auth.id

        distribution.occur_at = data.occur_at
        db.flush()

        # 改期搬移可能释放了原授权额度：按受理顺序重算本通道候补队首。
        _process_waitlist(db, region, channel)
        db.commit()
    except (ResourceNotFound, DistributionRevoked, RescheduleRejected):
        raise
    except Exception:
        db.rollback()
        raise

    return get_distribution(db, distribution_id=distribution_id)


# =============================================================== 额度预留
def create_reservation(db: Session, data: ReservationCreate) -> models.Reservation:
    """全部素材同时通过才生成预留编号并逐项占用预留额度；任一失败整体拒绝。"""
    material_ids = list(data.material_ids)
    _validate_material_ids(db, material_ids)

    now = utcnow()
    expires_at = now + timedelta(minutes=data.ttl_minutes)

    try:
        _channel_lock(db, data.region, data.channel)

        candidate_ids = _candidate_auth_ids(
            db, material_ids, data.region, data.channel, data.occur_at
        )
        locked_rows = _settle_expired(
            db,
            now,
            region=data.region,
            channel=data.channel,
            extra_auth_ids=candidate_ids,
        )

        # 沿用既有授权匹配与重叠选择顺序；未到期预留同样占额。
        chosen, reasons = _evaluate(
            material_ids,
            locked_rows,
            region=data.region,
            channel=data.channel,
            occur=data.occur_at,
            for_reservation=True,
        )
        if reasons:
            db.rollback()
            raise ReservationRejected(reasons)

        reservation = models.Reservation(
            region=data.region,
            channel=data.channel,
            occur_at=data.occur_at,
            expires_at=expires_at,
            ttl_minutes=data.ttl_minutes,
            status="pending",
        )
        db.add(reservation)
        db.flush()

        # 逐项条件预留：used + reserved < max 护栏，断言影响行数。
        # 素材已去重，同一授权在一次预留中至多被选中一次。
        for index, mid in enumerate(material_ids):
            auth = chosen[mid]
            result = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == auth.id,
                    models.Authorization.used_count
                    + models.Authorization.reserved_count
                    < models.Authorization.max_count,
                )
                .values(
                    reserved_count=models.Authorization.reserved_count + 1
                )
            )
            if result.rowcount != 1:
                db.rollback()
                raise ReservationRejected(
                    [
                        {
                            "item_index": index,
                            "material_id": mid,
                            "reason": "quota_exhausted",
                            "message": (
                                f"授权 id={auth.id} 可预留余量不足，"
                                "并发预留冲突，本次预留整体拒绝"
                            ),
                        }
                    ]
                )
            db.add(
                models.ReservationItem(
                    reservation_id=reservation.id,
                    material_id=mid,
                    authorization_id=auth.id,
                    item_index=index,
                )
            )
        db.commit()
    except ReservationRejected:
        raise
    except Exception:
        db.rollback()
        raise

    return _load_reservation(db, reservation.id)


def list_reservations(
    db: Session, status: str | None
) -> list[models.Reservation]:
    """查询预留列表，可按状态过滤。

    列表为只读视图：库内仍 pending 但已过到期时刻的预留按 **expired**
    呈现（写路径与单条查询会惰性结算落库；此处仅在响应中派生状态，
    不产生写事务）。
    """
    if status is not None and status not in (
        "pending",
        "confirmed",
        "cancelled",
        "expired",
    ):
        raise InvalidReservationStatus(status)

    now = utcnow()
    rows = list(
        db.scalars(
            select(models.Reservation)
            .options(
                selectinload(models.Reservation.items).selectinload(
                    models.ReservationItem.material
                ),
                selectinload(models.Reservation.items).selectinload(
                    models.ReservationItem.authorization
                ),
                selectinload(models.Reservation.distribution)
                .selectinload(models.Distribution.items)
                .selectinload(models.DistributionItem.material),
                selectinload(models.Reservation.distribution)
                .selectinload(models.Distribution.items)
                .selectinload(models.DistributionItem.authorization),
            )
            .order_by(models.Reservation.id.asc())
        )
    )

    result: list[models.Reservation] = []
    for resv in rows:
        effective = resv.status
        if resv.status == "pending" and resv.expires_at <= now:
            effective = "expired"
        if status is None or effective == status:
            # 分离后覆盖派生状态，避免把瞬时状态标记为脏对象。
            db.expunge(resv)
            object.__setattr__(resv, "status", effective)
            result.append(resv)
    return result


def _load_reservation(
    db: Session, reservation_id: int, *, for_update: bool = False
) -> models.Reservation:
    # 确认事务内把同事务新建的 distribution 赋给本预留后，关系缓存可能为
    # None；一律使会话已加载状态过期，保证下面 selectinload 真正载入新关系。
    db.expire_all()
    stmt = (
        select(models.Reservation)
        .where(models.Reservation.id == reservation_id)
        .options(
            selectinload(models.Reservation.items).selectinload(
                models.ReservationItem.material
            ),
            selectinload(models.Reservation.items).selectinload(
                models.ReservationItem.authorization
            ),
            selectinload(models.Reservation.distribution)
            .selectinload(models.Distribution.items)
            .selectinload(models.DistributionItem.material),
            selectinload(models.Reservation.distribution)
            .selectinload(models.Distribution.items)
            .selectinload(models.DistributionItem.authorization),
        )
    )
    if for_update:
        stmt = stmt.with_for_update()
    resv = db.scalar(stmt)
    if resv is None:
        raise ResourceNotFound("预留", reservation_id)
    return resv


def get_reservation(db: Session, reservation_id: int) -> models.Reservation:
    """查询预留；先在通道内结算到期预留，逾期者落库为 expired。"""
    head = db.execute(
        select(
            models.Reservation.region,
            models.Reservation.channel,
        ).where(models.Reservation.id == reservation_id)
    ).first()
    if head is None:
        raise ResourceNotFound("预留", reservation_id)
    region, channel = head
    try:
        _channel_lock(db, region, channel)
        _settle_expired(db, utcnow(), region=region, channel=channel)
        db.commit()
    except Exception:
        db.rollback()
        raise
    return _load_reservation(db, reservation_id)


def confirm_reservation(
    db: Session, reservation_id: int
) -> models.Reservation:
    """到期前确认：把预留原子转为一笔 approved 申请。

    - 重复确认返回同一笔申请（幂等，200）；
    - 已取消 / 已过期（含恰在到期时刻确认，视为逾期）→ 409；
    - 授权停用不影响确认：确认不重新匹配授权、不要求启用，只把预留槽位
      转为同一授权上的正式占用。
    """
    head = db.execute(
        select(
            models.Reservation.region,
            models.Reservation.channel,
        ).where(models.Reservation.id == reservation_id)
    ).first()
    if head is None:
        raise ResourceNotFound("预留", reservation_id)
    region, channel = head

    try:
        _channel_lock(db, region, channel)

        # 本预留涉及的授权行必须纳入统一行锁集合（条件转账需要）。
        item_auth_ids = list(
            db.scalars(
                select(models.ReservationItem.authorization_id)
                .join(
                    models.Reservation,
                    models.ReservationItem.reservation_id
                    == models.Reservation.id,
                )
                .where(models.Reservation.id == reservation_id)
            )
        )
        _settle_expired(
            db,
            utcnow(),
            region=region,
            channel=channel,
            extra_auth_ids=item_auth_ids,
        )

        resv = _load_reservation(db, reservation_id, for_update=True)

        if resv.status == "confirmed":
            # 幂等：重复确认返回同一申请，不重复占用。
            db.rollback()
            return _load_reservation(db, reservation_id)
        if resv.status == "cancelled":
            db.rollback()
            raise ReservationCancelled(reservation_id)
        if resv.status == "expired":
            db.rollback()
            raise ReservationExpired(reservation_id)

        # pending：通道结算已保证 now < expires_at（否则已置 expired）。
        now = utcnow()
        auth_ids = sorted(
            {item.authorization_id for item in resv.items}
        )

        # 生成一笔 approved 申请及逐项明细，归属预留时命中的同一授权。
        distribution = models.Distribution(
            region=resv.region,
            channel=resv.channel,
            occur_at=resv.occur_at,
            status="approved",
        )
        db.add(distribution)
        db.flush()
        for item in resv.items:
            db.add(
                models.DistributionItem(
                    distribution_id=distribution.id,
                    material_id=item.material_id,
                    authorization_id=item.authorization_id,
                    item_index=item.item_index,
                )
            )

        # 原子转账：reserved_count - 1（条件 >=1）紧接 used_count + 1
        # （条件 used + reserved < max，即槽位确实来自本预留），
        # 占用总量不变，绝不超额、不重复。
        for auth_id in auth_ids:
            dec = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == auth_id,
                    models.Authorization.reserved_count >= 1,
                )
                .values(
                    reserved_count=models.Authorization.reserved_count - 1
                )
            )
            if dec.rowcount != 1:
                db.rollback()
                raise RuntimeError(
                    f"确认预留失败：授权 {auth_id} reserved_count 异常"
                )
            inc = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == auth_id,
                    models.Authorization.used_count
                    + models.Authorization.reserved_count
                    < models.Authorization.max_count,
                )
                .values(used_count=models.Authorization.used_count + 1)
            )
            if inc.rowcount != 1:
                db.rollback()
                raise RuntimeError(
                    f"确认预留失败：授权 {auth_id} 正式余量不足"
                )

        resv.status = "confirmed"
        resv.distribution_id = distribution.id
        resv.confirmed_at = now
        db.commit()
    except (
        ResourceNotFound,
        ReservationCancelled,
        ReservationExpired,
    ):
        raise
    except Exception:
        db.rollback()
        raise

    return _load_reservation(db, reservation_id)


def cancel_reservation(db: Session, reservation_id: int) -> models.Reservation:
    """取消预留（与确认互斥）：逐项释放预留额度，仅生效一次。

    - 重复取消返回 409，不再释放额度；
    - 已确认 → 409（不可取消）；已过期 → 409（额度已由到期结算释放）。
    """
    head = db.execute(
        select(
            models.Reservation.region,
            models.Reservation.channel,
        ).where(models.Reservation.id == reservation_id)
    ).first()
    if head is None:
        raise ResourceNotFound("预留", reservation_id)
    region, channel = head

    try:
        _channel_lock(db, region, channel)

        item_auth_ids = list(
            db.scalars(
                select(models.ReservationItem.authorization_id)
                .join(
                    models.Reservation,
                    models.ReservationItem.reservation_id
                    == models.Reservation.id,
                )
                .where(models.Reservation.id == reservation_id)
            )
        )
        _settle_expired(
            db,
            utcnow(),
            region=region,
            channel=channel,
            extra_auth_ids=item_auth_ids,
        )

        resv = _load_reservation(db, reservation_id, for_update=True)

        if resv.status == "cancelled":
            db.rollback()
            raise ReservationAlreadyCancelled(reservation_id)
        if resv.status == "confirmed":
            db.rollback()
            raise ReservationAlreadyConfirmed(reservation_id)
        if resv.status == "expired":
            db.rollback()
            raise ReservationExpired(reservation_id, action="取消")

        auth_ids = sorted(
            {item.authorization_id for item in resv.items}
        )
        for auth_id in auth_ids:
            result = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == auth_id,
                    models.Authorization.reserved_count >= 1,
                )
                .values(
                    reserved_count=models.Authorization.reserved_count - 1
                )
            )
            if result.rowcount != 1:
                db.rollback()
                raise RuntimeError(
                    f"取消预留失败：授权 {auth_id} reserved_count 异常"
                )

        resv.status = "cancelled"
        resv.cancelled_at = utcnow()
        db.flush()

        # 取消预留释放额度后，按受理顺序重算本通道候补队首。
        _process_waitlist(db, region, channel)
        db.commit()
    except (
        ResourceNotFound,
        ReservationAlreadyCancelled,
        ReservationAlreadyConfirmed,
        ReservationExpired,
    ):
        raise
    except Exception:
        db.rollback()
        raise

    return _load_reservation(db, reservation_id)


# --------------------------------------------------------- 授权退役改挂
def migrate_authorization(
    db: Session, data: AuthorizationMigrationRequest
) -> dict:
    """原授权退役：把其承载的全部 **未撤销** 申请明细一次性改挂到替代授权。

    前置条件（任一不满足则整体拒绝，申请归属与两端次数均不变）：

    - 两端编号不同且授权均存在；
    - 素材、地区、渠道三个维度完全一致；
    - 替代授权为启用状态（active）；
    - **原授权不存在未到期的待确认预留**（预留仍指向原授权，停用会使
      确认语义与余量账目不清晰，故整体拒绝，原因 ``pending_reservations``；
      待预留确认/取消/到期后再改挂即可）；
    - 替代授权时段左闭右开覆盖每条待迁移申请的 ``occur_at``；
    - 替代授权余量（扣除其自身未到期预留后）足够一次性承接全部迁入占用。

    并发：单事务内「咨询锁 → 到期结算 → 授权行锁 → 申请行锁 → 明细行锁」，
    与提交/预留/确认/取消/撤销遵循同一加锁顺序；次数搬移全部使用条件更新
    并断言影响行数，绝不超额或重复释放。
    """
    source_id = data.source_authorization_id
    target_id = data.replacement_authorization_id

    if source_id == target_id:
        raise MigrationRejected(
            [
                {
                    "reason": "same_authorization",
                    "message": "原授权与替代授权必须为不同编号",
                    "source_authorization_id": source_id,
                    "replacement_authorization_id": target_id,
                }
            ]
        )

    def _reject(reason: str, message: str) -> MigrationRejected:
        return MigrationRejected(
            [
                {
                    "reason": reason,
                    "message": message,
                    "source_authorization_id": source_id,
                    "replacement_authorization_id": target_id,
                }
            ]
        )

    # 不加锁读取，仅用于存在性/维度判断与确定咨询锁桶；
    # 维度（material_id/region/channel）不可变，无需锁保护。
    source = db.get(models.Authorization, source_id)
    if source is None:
        raise ResourceNotFound("授权", source_id)
    target = db.get(models.Authorization, target_id)
    if target is None:
        raise ResourceNotFound("授权", target_id)

    try:
        # 1) 维度校验先行，不一致直接拒绝，不触碰任何锁。
        if source.region != target.region or source.channel != target.channel:
            raise _reject(
                "dimension_mismatch",
                "原授权与替代授权的地区、渠道不一致，不可改挂",
            )
        if source.material_id != target.material_id:
            raise _reject(
                "dimension_mismatch",
                "原授权与替代授权的素材不一致，不可改挂",
            )

        # 2) 第一加锁点：通道咨询锁；随后立即结算本通道到期预留
        #    （归还 reserved_count），再锁定两端授权行。
        _channel_lock(db, source.region, source.channel)
        locked_rows = _settle_expired(
            db,
            utcnow(),
            region=source.region,
            channel=source.channel,
            extra_auth_ids=[source_id, target_id],
        )
        by_id = {a.id: a for a in locked_rows}
        source = by_id.get(source_id, source)
        target = by_id.get(target_id, target)

        # 3) 替代授权必须启用。
        if target.status != "active":
            raise _reject(
                "replacement_inactive",
                f"替代授权（id={target_id}）未处于启用状态",
            )

        # 4) 原授权存在未到期（待确认）预留时整体拒绝。
        pending_resv_ids = list(
            db.scalars(
                select(models.Reservation.id)
                .join(
                    models.ReservationItem,
                    models.ReservationItem.reservation_id
                    == models.Reservation.id,
                )
                .where(
                    models.Reservation.status == "pending",
                    models.ReservationItem.authorization_id == source_id,
                )
            )
        )
        if pending_resv_ids:
            raise _reject(
                "pending_reservations",
                f"原授权（id={source_id}）存在 {len(pending_resv_ids)} 条"
                "未到期的待确认预留，请待其确认/取消/到期后再改挂",
            )

        # 5) 锁定原授权承载的全部未撤销申请的申请行（id 升序）。
        dist_rows = list(
            db.scalars(
                select(models.Distribution)
                .join(
                    models.DistributionItem,
                    models.DistributionItem.distribution_id
                    == models.Distribution.id,
                )
                .where(
                    models.DistributionItem.authorization_id == source_id,
                    models.Distribution.status == "approved",
                )
                .order_by(models.Distribution.id.asc())
                .with_for_update()
            )
        )
        dist_ids = [d.id for d in dist_rows]

        # 6) 锁定这些申请下归属原授权的明细行（FOR UPDATE）。
        items: list[models.DistributionItem] = []
        occur_times: list[datetime] = []
        if dist_ids:
            items = list(
                db.scalars(
                    select(models.DistributionItem)
                    .where(
                        models.DistributionItem.distribution_id.in_(
                            dist_ids
                        ),
                        models.DistributionItem.authorization_id
                        == source_id,
                    )
                    .order_by(models.DistributionItem.id.asc())
                    .with_for_update()
                )
            )
            occur_map = {d.id: d.occur_at for d in dist_rows}
            occur_times = [occur_map[it.distribution_id] for it in items]

        n = len(items)

        # 7) 逐条校验替代授权时段覆盖发行时刻（左闭右开）。
        uncovered = sorted(
            {
                it.id
                for it, occur_at in zip(items, occur_times)
                if not (target.starts_at <= occur_at < target.ends_at)
            }
        )
        if uncovered:
            raise _reject(
                "period_not_covered",
                f"替代授权（id={target_id}）时段未覆盖 {len(uncovered)} 条"
                "申请的发行时刻（需满足 starts_at <= occur_at < ends_at）",
            )

        # 8) 余量必须足够一次性承接全部迁入占用（扣除替代授权自身的
        #    未到期预留，预留与正式占用共享总额度）。
        if (
            target.used_count + target.reserved_count + n
            > target.max_count
        ):
            raise _reject(
                "insufficient_quota",
                f"替代授权（id={target_id}）余量不足：待迁入 {n} 条，"
                f"正式占用 {target.used_count}、未到期预留 "
                f"{target.reserved_count}/{target.max_count}",
            )

        # 9) 一次性搬移：明细归属改替代授权；原授权条件减 n；
        #    替代授权条件加 n（含预留护栏）；原授权停用。
        if n:
            moved = db.execute(
                update(models.DistributionItem)
                .where(
                    models.DistributionItem.id.in_([it.id for it in items]),
                    models.DistributionItem.authorization_id == source_id,
                )
                .values(authorization_id=target_id)
            )
            if moved.rowcount != n:
                db.rollback()
                raise RuntimeError(
                    f"改挂明细数异常：期望 {n}，实际 {moved.rowcount}"
                )

            dec = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == source_id,
                    models.Authorization.used_count >= n,
                )
                .values(
                    used_count=models.Authorization.used_count - n
                )
            )
            if dec.rowcount != 1:
                db.rollback()
                raise RuntimeError(
                    f"原授权 {source_id} 迁出次数异常（used_count < {n}）"
                )

            inc = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == target_id,
                    models.Authorization.used_count
                    + models.Authorization.reserved_count
                    <= models.Authorization.max_count - n,
                )
                .values(
                    used_count=models.Authorization.used_count + n
                )
            )
            if inc.rowcount != 1:
                db.rollback()
                raise RuntimeError(
                    f"替代授权 {target_id} 余量不足，迁入 {n} 条失败"
                )

        source.status = "inactive"
        db.flush()

        # 改挂后原授权停用、占用迁出：按受理顺序重算本通道候补队首
        # （仍匹配但额度释放的候补可成交；匹配授权已停用的候补标记失败）。
        _process_waitlist(db, source.region, source.channel)
        db.commit()
    except MigrationRejected:
        db.rollback()
        raise
    except Exception:
        db.rollback()
        raise

    db.refresh(source)
    db.refresh(target)
    return {
        "migrated_count": n,
        "source_authorization": source,
        "replacement_authorization": target,
    }


# ================================================================== 候补
def create_waitlist(db: Session, data: WaitlistCreate) -> models.Waitlist:
    """提交候补申请：沿用既有匹配与重叠授权选择规则。

    - 全部可承接：直接原子占额生成一笔 approved 申请，候补记为
      ``fulfilled`` 并关联申请编号；
    - 每项均有匹配授权但额度不足：进入本通道候补队列（``pending``，
      按受理顺序等待，不占任何额度）；
    - 任一项缺少匹配授权（无匹配或匹配授权均已停用）：整体逐项拒绝，
      不入队、不占额。
    """
    material_ids = list(data.material_ids)
    _validate_material_ids(db, material_ids)

    try:
        _channel_lock(db, data.region, data.channel)
        now = utcnow()

        candidate_ids = _candidate_auth_ids(
            db, material_ids, data.region, data.channel, data.occur_at
        )
        locked_rows = _settle_expired(
            db,
            now,
            region=data.region,
            channel=data.channel,
            extra_auth_ids=candidate_ids,
        )

        # 既有候补按受理顺序优先：先重算队首，新候补不得插队抢额。
        if _process_waitlist(db, data.region, data.channel):
            locked_rows = _lock_authorizations(db, candidate_ids)

        chosen, reasons = _evaluate(
            material_ids,
            locked_rows,
            region=data.region,
            channel=data.channel,
            occur=data.occur_at,
            for_reservation=False,
        )

        if reasons:
            # 任一项缺少匹配授权（无匹配/均已停用）→ 整体逐项拒绝；
            # 仅当每项均有匹配授权、纯粹额度不足时才入队候补。
            if any(r["reason"] != "quota_exhausted" for r in reasons):
                db.rollback()
                raise WaitlistRejected(reasons)
            waitlist = models.Waitlist(
                region=data.region,
                channel=data.channel,
                occur_at=data.occur_at,
                status="pending",
            )
            db.add(waitlist)
            db.flush()
            for index, mid in enumerate(material_ids):
                db.add(
                    models.WaitlistItem(
                        waitlist_id=waitlist.id,
                        material_id=mid,
                        item_index=index,
                    )
                )
            db.commit()
            return _load_waitlist(db, waitlist.id)

        # 全部可承接：与直接申请一致地条件占额并生成 approved 申请。
        for auth in chosen.values():
            result = db.execute(
                update(models.Authorization)
                .where(
                    models.Authorization.id == auth.id,
                    models.Authorization.used_count
                    + models.Authorization.reserved_count
                    < models.Authorization.max_count,
                )
                .values(used_count=models.Authorization.used_count + 1)
            )
            if result.rowcount != 1:
                db.rollback()
                raise WaitlistRejected(
                    [
                        {
                            "material_id": auth.material_id,
                            "reason": "quota_exhausted",
                            "message": (
                                f"授权 id={auth.id} 余量不足，"
                                "并发占用冲突，本次候补整体拒绝"
                            ),
                        }
                    ]
                )

        distribution = models.Distribution(
            region=data.region,
            channel=data.channel,
            occur_at=data.occur_at,
            status="approved",
        )
        db.add(distribution)
        db.flush()
        for index, mid in enumerate(material_ids):
            db.add(
                models.DistributionItem(
                    distribution_id=distribution.id,
                    material_id=mid,
                    authorization_id=chosen[mid].id,
                    item_index=index,
                )
            )
        waitlist = models.Waitlist(
            region=data.region,
            channel=data.channel,
            occur_at=data.occur_at,
            status="fulfilled",
            distribution_id=distribution.id,
            fulfilled_at=now,
        )
        db.add(waitlist)
        db.flush()
        db.commit()
    except WaitlistRejected:
        raise
    except Exception:
        db.rollback()
        raise

    return _load_waitlist(db, waitlist.id)


def _load_waitlist(
    db: Session, waitlist_id: int, *, for_update: bool = False
) -> models.Waitlist:
    # 成交事务内把同事务新建的 distribution 赋给本候补后，关系缓存可能为
    # None；一律使会话已加载状态过期，保证下面 selectinload 真正载入新关系。
    db.expire_all()
    stmt = (
        select(models.Waitlist)
        .where(models.Waitlist.id == waitlist_id)
        .options(
            selectinload(models.Waitlist.items).selectinload(
                models.WaitlistItem.material
            ),
            selectinload(models.Waitlist.distribution)
            .selectinload(models.Distribution.items)
            .selectinload(models.DistributionItem.material),
            selectinload(models.Waitlist.distribution)
            .selectinload(models.Distribution.items)
            .selectinload(models.DistributionItem.authorization),
        )
    )
    if for_update:
        stmt = stmt.with_for_update()
    waitlist = db.scalar(stmt)
    if waitlist is None:
        raise ResourceNotFound("候补", waitlist_id)
    return waitlist


def get_waitlist(db: Session, waitlist_id: int) -> models.Waitlist:
    """查询候补；先在通道内结算到期预留（可能顺带成交本通道候补）。"""
    head = db.execute(
        select(
            models.Waitlist.region,
            models.Waitlist.channel,
        ).where(models.Waitlist.id == waitlist_id)
    ).first()
    if head is None:
        raise ResourceNotFound("候补", waitlist_id)
    region, channel = head
    try:
        _channel_lock(db, region, channel)
        _settle_expired(db, utcnow(), region=region, channel=channel)
        db.commit()
    except Exception:
        db.rollback()
        raise
    return _load_waitlist(db, waitlist_id)


def list_waitlists(
    db: Session, status: str | None
) -> list[models.Waitlist]:
    """查询候补列表（按受理顺序），可按状态过滤。"""
    if status is not None and status not in (
        "pending",
        "fulfilled",
        "failed",
        "cancelled",
    ):
        raise InvalidWaitlistStatus(status)

    stmt = (
        select(models.Waitlist)
        .options(
            selectinload(models.Waitlist.items).selectinload(
                models.WaitlistItem.material
            ),
            selectinload(models.Waitlist.distribution)
            .selectinload(models.Distribution.items)
            .selectinload(models.DistributionItem.material),
            selectinload(models.Waitlist.distribution)
            .selectinload(models.Distribution.items)
            .selectinload(models.DistributionItem.authorization),
        )
        .order_by(models.Waitlist.id.asc())
    )
    if status is not None:
        stmt = stmt.where(models.Waitlist.status == status)
    return list(db.scalars(stmt))


def cancel_waitlist(db: Session, waitlist_id: int) -> models.Waitlist:
    """取消待成交候补（仅 pending 可取消，仅生效一次）。

    候补在队列中不占任何额度，取消无需归还；已成交的候补不可取消，
    其生成的申请沿用既有撤销规则（``POST /distributions/{id}/revoke``）。
    """
    head = db.execute(
        select(
            models.Waitlist.region,
            models.Waitlist.channel,
        ).where(models.Waitlist.id == waitlist_id)
    ).first()
    if head is None:
        raise ResourceNotFound("候补", waitlist_id)
    region, channel = head

    try:
        _channel_lock(db, region, channel)
        _settle_expired(db, utcnow(), region=region, channel=channel)

        waitlist = _load_waitlist(db, waitlist_id, for_update=True)
        if waitlist.status == "fulfilled":
            db.rollback()
            raise WaitlistAlreadyFulfilled(
                waitlist_id, waitlist.distribution_id
            )
        if waitlist.status == "cancelled":
            db.rollback()
            raise WaitlistAlreadyCancelled(waitlist_id)
        if waitlist.status == "failed":
            db.rollback()
            raise WaitlistAlreadyFailed(waitlist_id)

        waitlist.status = "cancelled"
        waitlist.cancelled_at = utcnow()
        db.flush()

        # 队首可能因本次取消而变化：按受理顺序重算本通道候补队首。
        _process_waitlist(db, region, channel)
        db.commit()
    except (ResourceNotFound, WaitlistNotPending):
        raise
    except Exception:
        db.rollback()
        raise

    return _load_waitlist(db, waitlist_id)
