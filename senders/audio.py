"""
Audio Sender Plugin - Campañas de audio con FreeSWITCH
Reproduce mensajes de audio automáticos a los destinatarios.
"""
import asyncio
import time
import threading
from typing import Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
from datetime import datetime, time as dt_time

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
        self.active_numbers: Set[Tuple[str, str]] = set()
        
        # Configuración específica
        self.amd_type = "PRO"
        self.audio_file = None
        self.max_concurrent = GLOBAL_MAX_CONCURRENT_CALLS
        self.cps = CPS_GLOBAL
    
    async def initialize(self) -> bool:
        """Inicializa conexión ESL"""
        if not ESL_AVAILABLE:
            self.logger.error("❌ ESL no disponible")
            return False
        
        try:
            self.esl_connection = ESL.ESLconnection(
                FREESWITCH_HOST, 
                FREESWITCH_PORT, 
                FREESWITCH_PASSWORD
            )
            
            if not self.esl_connection.connected():
                self.logger.error("❌ No se pudo conectar a FreeSWITCH ESL")
                return False
            
            self.logger.info(f"✅ Conectado a FreeSWITCH ESL ({FREESWITCH_HOST}:{FREESWITCH_PORT})")
            
            # Obtener configuración de campaña
            config = self.get_campaign_config()
            self.amd_type = config.get("amd", "PRO")
            self.cps = config.get("cps", CPS_GLOBAL)
            
            # Obtener audio de la campaña
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT audio FROM campanas WHERE nombre = :nombre
                """), {"nombre": self.campaign_name}).fetchone()
                
                if result and result[0]:
                    self.audio_file = f"{RECORDINGS_DIR}/{result[0]}"
                    self.logger.info(f"🔊 Audio: {self.audio_file}")
                else:
                    self.logger.warning("⚠️ No se encontró audio configurado")
            
            self.stats.rate_max = self.cps
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
            # Con AMD: detecta y luego transfiere al IVR
            return (
                f"bgapi originate "
                f"{{ignore_early_media=false,"
                f"origination_uuid={uuid},"
                f"campaign_name='{self.campaign_name}',"
                f"campaign_type='Discador',"
                f"dialer_queue='{self.queue_relationated}',"
                f"origination_caller_id_number='{numero}',"
                f"execute_on_answer='transfer 9999 XML {self.campaign_name}'}}"
                f"sofia/gateway/{GATEWAY}/{numero} 2222 XML DETECT_AMD_PRO"
            )
        else:
            # Sin AMD: transfer directo al IVR
            return (
                f"bgapi originate "
                f"{{ignore_early_media=false,"
                f"origination_uuid={uuid},"
                f"campaign_name='{self.campaign_name}',"
                f"campaign_type='Discador',"
                f"dialer_queue='{self.queue_relationated}',"
                f"origination_caller_id_number='{numero}',"
                f"execute_on_answer='transfer 9999 XML {self.campaign_name}'}}"
                f"sofia/gateway/{GATEWAY}/{numero} &park()"
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
            
            if response:
                body = response.getBody()
                success = "+OK" in body or "Job-UUID" in body
                
                if success:
                    return (numero, True, {"uuid": uuid, "status": "sent"})
                else:
                    # Falló - actualizar estado
                    self.release_active(numero)
                    with self.engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET estado = 'F', hangup_cause = 'ORIGINATE_FAILED'
                            WHERE uuid = :uuid
                        """), {"uuid": uuid})
                    return (numero, False, {"error": body[:100]})
            
            self.release_active(numero)
            return (numero, False, {"error": "no_response"})
            
        except Exception as e:
            self.release_active(numero)
            return (numero, False, {"error": str(e)})
    
    async def send_batch(self, items: list) -> list:
        """Envía un lote de llamadas"""
        results = []
        
        # Preparar batch
        batch_data = []
        for item in items:
            numero = item.get('telefono', '')
            if not numero or not numero.strip().isdigit():
                results.append((numero, False, {"error": "invalid"}))
                continue
            
            if self.is_active(numero):
                continue
            
            uuid = f"audio_{self.campaign_name}_{numero}_{int(time.time()*1000000)}"
            originate_str = self._build_originate_string(numero, uuid)
            batch_data.append((numero, uuid, originate_str))
            
            # Registrar
            self.register_active(numero)
            self.active_uuids.add(uuid)
        
        # Actualizar BD en batch
        if batch_data:
            try:
                with self.engine.begin() as conn:
                    for numero, uuid, _ in batch_data:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET uuid = :uuid, estado = 'P', fecha_envio = NOW(),
                                intentos = COALESCE(intentos, 0) + 1
                            WHERE telefono = :numero AND estado NOT IN ('S', 'C', 'P')
                        """), {"uuid": uuid, "numero": numero})
            except Exception as e:
                self.logger.error(f"Error batch update: {e}")
        
        # Enviar originates
        loop = asyncio.get_event_loop()
        
        MINI_BATCH = 5
        DELAY = 0.35
        
        for i in range(0, len(batch_data), MINI_BATCH):
            mini = batch_data[i:i+MINI_BATCH]
            
            async def send_one(data):
                numero, uuid, originate_str = data
                try:
                    response = await loop.run_in_executor(
                        self.esl_executor,
                        lambda: self.esl_connection.api(originate_str)
                    )
                    if response:
                        body = response.getBody()
                        success = "+OK" in body or "Job-UUID" in body
                        return (numero, success, {"uuid": uuid})
                    return (numero, False, {"error": "no_response"})
                except Exception as e:
                    return (numero, False, {"error": str(e)})
            
            batch_results = await asyncio.gather(*[send_one(d) for d in mini])
            
            for r in batch_results:
                results.append(r)
                if not r[1]:  # Failed
                    self.release_active(r[0])
            
            if i + MINI_BATCH < len(batch_data):
                await asyncio.sleep(DELAY)
        
        return results
    
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
                    SELECT DISTINCT telefono, nombre, datos
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
                        items.append({
                            "telefono": numero,
                            "nombre": row[1] if len(row) > 1 else None,
                            "datos": row[2] if len(row) > 2 else None,
                        })
                
                return items
        except Exception as e:
            self.logger.error(f"Error obteniendo pendientes: {e}")
            return []
