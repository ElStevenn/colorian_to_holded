# SyncTrigger/__init__.py  (sólo esto)

import logging
import azure.functions as func
from src.services.sync_service import migration_proceed  # tu servicio real

async def main(mytimer: func.TimerRequest):
    logging.info("SyncTrigger: inicio")
    try:
        await migration_proceed()
    except Exception as exc:
        logging.exception("SyncTrigger falló: %s", exc)
        raise
    logging.info("SyncTrigger: fin OK")
