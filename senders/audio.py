"""
Audio Sender Plugin - Campañas de audio con FreeSWITCH
Reproduce mensajes de audio automáticos a los destinatarios.
Basado en el patrón de call_sender.py para máximo rendimiento.
"""
import asyncio
import time
from typing import Dict, Set, Optional
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text

from .base import BaseSender, SenderStats

# Importar configuración compartida
from shared_config import (
    FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD, GATEWAY,
    RECORDINGS_DIR, GLOBAL_MAX_CONCURRENT_CALLS, CPS_GLOBAL,
    TERMINAL_STATES, get_logger
)

# ESL
try:
    import ESL
    ESL_AVAILABLE = True
except ImportError:
    ESL_AVAILABLE = False

# Redis opcional
try:
    from redis_manager import get_redis_manager
    redis_manager = get_redis_manager()
    REDIS_AVAILABLE = redis_manager.ping()
except:
    REDIS_AVAILABLE = False
    redis_manager = None

logger = get_logger("audio_sender")

# Mapeo Hangup-Cause a estados
HANGUP_CAUSE_TO_STATE = {
    "NORMAL_CLEARING": "C",
    "ORIGINATOR_CANCEL": "C",
    "NO_ANSWER": "N",
    "NO_USER_RESPONSE": "N",
    "PROGRESS_TIMEOUT": "N",
    "RECOVERY_ON_TIMER_EXPIRE": "N",
    "USER_BUSY": "O",
    "NORMAL_CIRCUIT_CONGESTION": "O",
    "CALL_REJECTED": "R",
    "USER_NOT_REGISTERED": "R",
    "UNALLOCATED_NUMBER": "I",
    "INVALID_NUMBER_FORMAT": "I",
    "NO_ROUTE_DESTINATION": "I",
    "DESTINATION_OUT_OF_ORDER": "E",
    "NETWORK_OUT_OF_ORDER": "E",
    "NORMAL_TEMPORARY_FAILURE": "E",
    "ALLOTTED_TIMEOUT": "T",
    "MEDIA_TIMEOUT": "T",
    "MACHINE_DETECTED": "U",
}


class AudioSender(BaseSender):
    """
    Sender para campañas de Audio.
    Origina llamadas con FreeSWITCH y reproduce un audio.
    """
    
    CAMPAIGN_TYPE = "Audio"
    TERMINAL_STATES = ('S', 'C', 'X')
    RETRY_STATES = ('N', 'O', 'F', 'E', 'T', 'R', 'I')
    
    def __init__(self, campaign_name: str, config: dict = None):
        super().__init__(campaign_name, config)
        self.esl_connection = None
        self.esl_executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="audio_esl")
        
        # Tracking específico de Audio
        self.active_uuids: Set[str] = set()
        self.uuid_timestamps: Dict[str, float] = {}
        
        # Configuración específica
        self.amd_type = "PRO"
        self.audio_file = None
        self.destino = "9999"  # Destino para transferir después de AMD
        self.max_concurrent = GLOBAL_MAX_CONCURRENT_CALLS
        self.cps = config.get("cps", CPS_GLOBAL) if config else CPS_GLOBAL
    
    async def initialize(self) -> bool:
        """Inicializa conexión ESL"""
        if not ESL_AVAILABLE:
            self.logger.error("❌ ESL no disponible")
            return False
        
        try:
            self.esl_connection = ESL.ESLconnection(
                FREESWITCH_HOST, 
                str(FREESWITCH_PORT), 
                FREESWITCH_PASSWORD
            )
            
            if not self.esl_connection.connected():
                self.logger.error("❌ No se pudo conectar a FreeSWITCH ESL")
                return False
            
            self.logger.info(f"✅ Conectado a FreeSWITCH ESL ({FREESWITCH_HOST}:{FREESWITCH_PORT})")
            
            # Obtener configuración de campaña
            config = self.get_campaign_config()
            self.cps = config.get("cps", self.cps)
            
            # Obtener audio de la campaña desde la columna archsubido
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT archsubido FROM campanas WHERE nombre = :nombre
                """), {"nombre": self.campaign_name}).fetchone()
                
                if result and result[0]:
                    self.audio_file = f"{RECORDINGS_DIR}/{result[0]}"
                    self.logger.info(f"🔊 Audio: {self.audio_file}")
                else:
                    self.logger.warning("⚠️ No se encontró audio configurado")
            
            self.stats.rate_max = self.cps
            self.logger.info(f"📊 CPS configurado: {self.cps}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inicializando: {e}")
            return False
    
    async def cleanup(self):
        """Limpia recursos"""
        try:
            if self.esl_connection and self.esl_connection.connected():
                self.esl_connection.disconnect()
            self.esl_executor.shutdown(wait=False)
        except:
            pass
    
    def _build_originate_string(self, numero: str, uuid: str) -> str:
        """Construye el string de originate para FreeSWITCH"""
        if self.amd_type and self.amd_type.upper() == "PRO":
            # Con AMD PRO: detecta y luego transfiere al dialplan que reproduce audio
            return (
                f"bgapi originate "
                f"{{ignore_early_media=false,"
                f"origination_uuid={uuid},"
                f"campaign_name='{self.campaign_name}',"
                f"campaign_type='Audio',"
                f"origination_caller_id_number='{numero}',"
                f"execute_on_answer='transfer {self.destino} XML {self.campaign_name}'}}"
                f"sofia/gateway/{GATEWAY}/{numero} 2222 XML DETECT_AMD_PRO"
            )
        else:
            # Sin AMD - directo al audio
            return (
                    f"bgapi originate "
                    f"{{ignore_early_media=false,"
                    f"origination_uuid={request.uuid},"
                    f"campaign_name='{request.campaign_name}',"
                    f"origination_caller_id_number='{request.numero}',"
                    f"execute_on_answer='transfer {request.destino} XML {request.campaign_name}'}}"
                    f"sofia/gateway/{GATEWAY}/{request.numero} &park()"
                )
    
    async def send_single(self, item: dict) -> tuple:
        """Envía una sola llamada"""
        numero = item.get('telefono', '')
        
        if not numero or not numero.strip().isdigit():
            return (numero, False, {"error": "invalid_number"})
        
        # Verificar si ya está activo
        if self.is_active(numero):
            return (numero, False, {"error": "already_active"})
        
        uuid = f"audio_{self.campaign_name}_{numero}_{int(time.time()*1000000)}"
        
        # Registrar como activo
        self.register_active(numero)
        self.active_uuids.add(uuid)
        self.uuid_timestamps[uuid] = time.time()
        
        # Actualizar BD
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(f"""
                    UPDATE `{self.campaign_name}` 
                    SET uuid = :uuid, 
                        estado = 'P',
                        fecha_envio = NOW(),
                        intentos = COALESCE(intentos, 0) + 1
                    WHERE telefono = :numero
                    AND estado NOT IN ('S', 'C', 'P')
                """), {"uuid": uuid, "numero": numero})
                
                if result.rowcount == 0:
                    self.release_active(numero)
                    return (numero, False, {"error": "already_processed"})
        except Exception as e:
            self.release_active(numero)
            return (numero, False, {"error": str(e)})
        
        # Enviar originate
        originate_str = self._build_originate_string(numero, uuid)
        
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                self.esl_executor,
                lambda: self.esl_connection.api(originate_str)
            )
            
            success = False
            error_msg = ""
            
            if response:
                body = response.getBody()
                success = "+OK" in body or "Job-UUID" in body
                
                if success:
                    self.logger.info(f"📞 [{self.campaign_name}] Llamada enviada: {numero} | UUID: {uuid}")
                    
                    # Registrar en Redis
                    if REDIS_AVAILABLE and redis_manager:
                        try:
                            redis_manager.register_call_sent(self.campaign_name)
                        except:
                            pass
                    
                    return (numero, True, {"uuid": uuid, "status": "sent"})
                else:
                    error_msg = body[:100] if body else "Respuesta vacía"
                    self.logger.warning(f"⚠️ [{self.campaign_name}] Llamada rechazada {numero}: {error_msg}")
            else:
                error_msg = "Sin respuesta de FreeSWITCH"
                self.logger.error(f"❌ [{self.campaign_name}] Sin respuesta FS para {numero}")
            
            # Falló - actualizar estado
            self.release_active(numero)
            self.active_uuids.discard(uuid)
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                    UPDATE `{self.campaign_name}` 
                    SET estado = 'F', hangup_cause = 'ORIGINATE_FAILED'
                    WHERE uuid = :uuid
                """), {"uuid": uuid})
            return (numero, False, {"error": error_msg})
            
        except Exception as e:
            self.release_active(numero)
            self.active_uuids.discard(uuid)
            return (numero, False, {"error": str(e)})
    
    async def send_batch(self, items: list) -> list:
        """Envía un lote de llamadas con control de CPS"""
        results = []
        delay = 1.0 / self.cps if self.cps > 0 else 0.1
        
        self.logger.info(f"📞 [{self.campaign_name}] Enviando batch de {len(items)} llamadas a {self.cps} CPS")
        
        for i, item in enumerate(items):
            numero = item.get('telefono', '')
            if not numero or not numero.strip().isdigit():
                results.append((numero, False, {"error": "invalid"}))
                continue
            
            if self.is_active(numero):
                continue
            
            # Control de CPS
            await asyncio.sleep(delay)
            
            # Verificar límite de concurrencia
            while len(self.active_uuids) >= self.max_concurrent:
                await asyncio.sleep(0.1)
                self._cleanup_stale_uuids()
            
            # Enviar llamada
            result = await self.send_single(item)
            results.append(result)
            
            # Log progreso cada 50 llamadas
            if (i + 1) % 50 == 0:
                self.logger.info(f"📊 [{self.campaign_name}] Progreso: {i+1}/{len(items)}")
        
        return results
    
    def _cleanup_stale_uuids(self, max_age: float = 60.0):
        """Limpia UUIDs obsoletos"""
        current_time = time.time()
        stale = [uuid for uuid, ts in self.uuid_timestamps.items() 
                 if current_time - ts > max_age]
        for uuid in stale:
            self.active_uuids.discard(uuid)
            self.uuid_timestamps.pop(uuid, None)
    
    async def check_status(self, item_id: str) -> dict:
        """Verifica estado de una llamada"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT estado, hangup_cause, fecha_envio, fecha_entrega
                    FROM `{self.campaign_name}`
                    WHERE telefono = :numero
                """), {"numero": item_id}).fetchone()
                
                if result:
                    return {
                        "estado": result[0],
                        "hangup_cause": result[1],
                        "fecha_envio": str(result[2]) if result[2] else None,
                        "fecha_entrega": str(result[3]) if result[3] else None,
                    }
        except:
            pass
        return {"estado": "unknown"}
    
    async def get_pending_items(self, max_items: int = 100) -> list:
        """Obtiene números pendientes"""
        try:
            config = self.get_campaign_config()
            max_retries = config.get("max_retries", 3)
            
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT DISTINCT telefono
                    FROM `{self.campaign_name}`
                    WHERE (
                        estado = 'pendiente'
                        OR (estado IN ('N', 'O', 'F', 'E', 'T', 'R', 'I') 
                            AND (intentos IS NULL OR intentos < :max_retries))
                    )
                    AND estado NOT IN ('S', 'C', 'P')
                    LIMIT :limit
                """), {"max_retries": max_retries, "limit": max_items})
                
                items = []
                for row in result:
                    numero = row[0]
                    if not self.is_active(numero):
                        items.append({"telefono": numero})
                
                return items
        except Exception as e:
            self.logger.error(f"Error obteniendo pendientes: {e}")
            return []
