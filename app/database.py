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
    ``ck_auth_reserved_invariant`` 约束；新建库由 ``create_all`` 直接
    建出，旧库则在此幂等补齐（列存在/约束存在时跳过）。
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


def get_db():
    """FastAPI 依赖：每请求一个事务会话。"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
