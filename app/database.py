"""数据库引擎、会话工厂与建表初始化。"""
import time

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.config import settings


class Base(DeclarativeBase):
    pass


engine = create_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def init_db() -> None:
    """等待数据库就绪并创建全部表（幂等，重复执行无副作用）。

    由应用启动事件调用，因此 ``docker compose up`` 即自动初始化数据库，
    无需手工执行迁移脚本。
    """
    # 确保模型已注册到 metadata，再导入以避免循环导入。
    from app import models  # noqa: F401

    deadline = time.monotonic() + settings.db_wait_timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            Base.metadata.create_all(bind=engine)
            _upgrade_existing_tables(bind=engine)
            return
        except OperationalError as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"等待数据库就绪超时：{last_error}")


def _upgrade_existing_tables(bind) -> None:
    """对旧版本库做幂等补齐（无需手工迁移）。

    预留功能为 ``authorizations`` 增加了 ``reserved_count`` 列与
    ``ck_auth_reserved_invariant`` 约束；候补有效期功能为 ``waitlists``
    增加了 ``ttl_minutes`` / ``expires_at`` / ``expire_reason`` /
    ``expired_at`` 列与 ``expired`` 状态及相关约束。新建库由
    ``create_all`` 直接建出，旧库则在此幂等补齐（列/约束存在时跳过）。
    """
    from sqlalchemy import text

    with bind.begin() as conn:
        has_col = conn.execute(
            text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name='authorizations' AND column_name='reserved_count'"
            )
        ).first()
        if not has_col:
            conn.execute(
                text(
                    "ALTER TABLE authorizations "
                    "ADD COLUMN reserved_count INTEGER NOT NULL DEFAULT 0"
                )
            )
        has_ck = conn.execute(
            text(
                "SELECT 1 FROM pg_constraint WHERE conname="
                "'ck_auth_reserved_invariant'"
            )
        ).first()
        if not has_ck:
            conn.execute(
                text(
                    "ALTER TABLE authorizations "
                    "ADD CONSTRAINT ck_auth_reserved_invariant "
                    "CHECK (reserved_count >= 0 "
                    "AND used_count + reserved_count <= max_count)"
                )
            )

        _upgrade_waitlists(conn, text)


def _upgrade_waitlists(conn, text) -> None:
    """旧库幂等补齐候补有效期（ttl_minutes/expires_at/expire_reason/expired_at）。

    既有终态候补（fulfilled/failed/cancelled）的 ``expires_at`` 回填为
    各自 ``created_at``；状态约束重建为含 ``expired`` 的新版本。
    """
    has_table = conn.execute(
        text("SELECT 1 FROM information_schema.tables WHERE table_name='waitlists'")
    ).first()
    if not has_table:
        # 旧版本无候补表：create_all 已按最新模型建出，无需补齐。
        return

    cols = {
        row[0]
        for row in conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='waitlists'"
            )
        )
    }
    if "ttl_minutes" not in cols:
        # 既有候补按有效期上限 1440 分钟回填（历史数据的失效时刻随之回填
        # 为 created_at + 1440 分钟），随后去掉列默认值（新行由应用层显式
        # 写入）。
        conn.execute(
            text(
                "ALTER TABLE waitlists "
                "ADD COLUMN ttl_minutes INTEGER NOT NULL DEFAULT 1440"
            )
        )
        conn.execute(
            text("ALTER TABLE waitlists ALTER COLUMN ttl_minutes DROP DEFAULT")
        )
    if "expires_at" not in cols:
        conn.execute(text("ALTER TABLE waitlists ADD COLUMN expires_at TIMESTAMPTZ"))
        conn.execute(
            text(
                "UPDATE waitlists SET expires_at = created_at + "
                "make_interval(mins => ttl_minutes) "
                "WHERE expires_at IS NULL"
            )
        )
        conn.execute(
            text("ALTER TABLE waitlists ALTER COLUMN expires_at SET NOT NULL")
        )
    if "expire_reason" not in cols:
        conn.execute(
            text("ALTER TABLE waitlists ADD COLUMN expire_reason VARCHAR(64)")
        )
    if "expired_at" not in cols:
        conn.execute(
            text("ALTER TABLE waitlists ADD COLUMN expired_at TIMESTAMPTZ")
        )

    # 状态约束旧版本不含 expired：存在则按新版本重建（IF EXISTS 幂等）。
    conn.execute(text("ALTER TABLE waitlists DROP CONSTRAINT IF EXISTS ck_wait_status"))
    conn.execute(
        text(
            "ALTER TABLE waitlists ADD CONSTRAINT ck_wait_status "
            "CHECK (status IN ('pending', 'fulfilled', 'failed', "
            "'cancelled', 'expired'))"
        )
    )
    if not conn.execute(
        text("SELECT 1 FROM pg_constraint WHERE conname='ck_wait_ttl_range'")
    ).first():
        conn.execute(
            text(
                "ALTER TABLE waitlists ADD CONSTRAINT ck_wait_ttl_range "
                "CHECK (ttl_minutes >= 1 AND ttl_minutes <= 1440)"
            )
        )
    if not conn.execute(
        text("SELECT 1 FROM pg_constraint WHERE conname='ck_wait_expired_at'")
    ).first():
        conn.execute(
            text(
                "ALTER TABLE waitlists ADD CONSTRAINT ck_wait_expired_at "
                "CHECK ((status = 'expired') = (expired_at IS NOT NULL))"
            )
        )


def get_db():
    """FastAPI 依赖：每请求一个事务会话。"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
