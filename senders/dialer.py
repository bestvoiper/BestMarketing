"""
Dialer Sender Plugin - Marcador predictivo con FreeSWITCH
Origina llamadas y las transfiere a colas de agentes.
"""
import asyncio
import time
import threading
from typing import Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text

from .base import BaseSender, SenderStats

# Importar configuración compartida
from shared_config import (
    FREESWITCH_HOST, FREESWITCH_PORT, FREESWITCH_PASSWORD, GATEWAY,
    DIALER_MAX_CONCURRENT_CALLS, DIALER_CPS_GLOBAL,
    DIALER_OVERDIAL_RATIO, DIALER_MIN_AGENTS, DIALER_ABANDON_THRESHOLD,
    DIALER_DEFAULT_QUEUE, DIALER_DEFAULT_CONTEXT,
    DIALER_STATES, DIALER_TERMINAL_STATES,
    get_logger
)

# ESL
try:
    import ESL
    ESL_AVAILABLE = True
except ImportError:
    ESL_AVAILABLE = False

logger = get_logger("dialer_sender")

# Mapeo Hangup-Cause a estados Discador
HANGUP_CAUSE_TO_DIALER_STATE = {
    "NORMAL_CLEARING": "C",
    "ORIGINATOR_CANCEL": "C",
    "NO_ANSWER": "N",
    "NO_USER_RESPONSE": "N",
    "PROGRESS_TIMEOUT": "N",
    "USER_BUSY": "O",
    "NORMAL_CIRCUIT_CONGESTION": "O",
    "CALL_REJECTED": "F",
    "USER_NOT_REGISTERED": "F",
    "UNALLOCATED_NUMBER": "F",
    "INVALID_NUMBER_FORMAT": "F",
    "NO_ROUTE_DESTINATION": "F",
    "DESTINATION_OUT_OF_ORDER": "F",
    "NETWORK_OUT_OF_ORDER": "F",
    "MACHINE_DETECTED": "M",
}


class DialerSender(BaseSender):
    """
    Sender para campañas de Discador (Marcador Predictivo).
    Origina llamadas y las transfiere a colas de agentes.
    """
    
    CAMPAIGN_TYPE = "Discador"
    TERMINAL_STATES = DIALER_TERMINAL_STATES
    RETRY_STATES = ('N', 'O', 'F', 'E', 'X')
    
    def __init__(self, campaign_name: str, config: dict = None):
        super().__init__(campaign_name, config)
        self.esl_connection = None
        self.esl_executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="dialer_esl")
        
        # Tracking
        self.active_uuids: Set[str] = set()
        self.uuid_timestamps: Dict[str, float] = {}
        
        # Configuración Discador
        self.cola_destino = DIALER_DEFAULT_QUEUE
        self.contexto = DIALER_DEFAULT_CONTEXT
        self.overdial_ratio = DIALER_OVERDIAL_RATIO
        self.amd_type = "PRO"
        self.max_concurrent = DIALER_MAX_CONCURRENT_CALLS
        self.cps = DIALER_CPS_GLOBAL
        
        # Métricas de agentes
        self.available_agents = 0
        self.total_agents = 0
        self.abandon_count = 0
        self.answered_count = 0
    
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
            
            self.logger.info(f"✅ Conectado a FreeSWITCH ESL")
            
            # Obtener configuración de campaña
            config = self.get_campaign_config()
            self.cola_destino = config.get("queue", DIALER_DEFAULT_QUEUE)
            self.contexto = config.get("context", DIALER_DEFAULT_CONTEXT)
            self.amd_type = config.get("amd", "PRO")
            self.cps = config.get("cps", DIALER_CPS_GLOBAL)
            
            # Obtener overdial
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text("""
                        SELECT overdial_ratio FROM campanas WHERE nombre = :nombre
                    """), {"nombre": self.campaign_name}).fetchone()
                    if result and result[0]:
                        self.overdial_ratio = float(result[0])
            except:
                pass
            
            self.logger.info(f"📞 Cola: {self.cola_destino} | Contexto: {self.contexto}")
            self.logger.info(f"📊 Overdial: {self.overdial_ratio} | CPS: {self.cps}")
            
            self.stats.rate_max = self.cps
            self.stats.extra_data["overdial_ratio"] = self.overdial_ratio
            self.stats.extra_data["cola_destino"] = self.cola_destino
            
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
    
    async def get_available_agents(self) -> dict:
        """Obtiene agentes disponibles para la cola"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN estado = 'disponible' THEN 1 ELSE 0 END) as disponibles,
                        SUM(CASE WHEN estado = 'en_llamada' THEN 1 ELSE 0 END) as ocupados
                    FROM agentes
                    WHERE cola = :cola AND activo = 'S'
                """), {"cola": self.cola_destino}).fetchone()
                
                if result:
                    self.total_agents = result[0] or 0
                    self.available_agents = result[1] or 0
                    return {
                        "total": self.total_agents,
                        "available": self.available_agents,
                        "busy": result[2] or 0
                    }
        except:
            pass
        
        # Default si no hay tabla agentes
        self.total_agents = 5
        self.available_agents = 3
        return {"total": 5, "available": 3, "busy": 2}
    
    def calculate_abandon_rate(self) -> float:
        """Calcula tasa de abandono"""
        total = self.answered_count + self.abandon_count
        if total > 0:
            return (self.abandon_count / total) * 100
        return 0.0
    
    def adjust_overdial(self):
        """Ajusta el overdial según abandono"""
        abandon_rate = self.calculate_abandon_rate()
        if abandon_rate > DIALER_ABANDON_THRESHOLD:
            self.overdial_ratio = max(1.0, self.overdial_ratio - 0.1)
            self.logger.info(f"📉 Reduciendo overdial a {self.overdial_ratio:.2f}")
        self.stats.extra_data["abandon_rate"] = abandon_rate
    
    def _build_originate_string(self, numero: str, uuid: str) -> str:
        """Construye el originate con transfer a cola"""
        if self.amd_type and self.amd_type.upper() == "PRO":
            # Con AMD: detecta y luego transfiere
            return (
                f"bgapi originate "
                f"{{ignore_early_media=false,"
                f"origination_uuid={uuid},"
                f"campaign_name='{self.campaign_name}',"
                f"campaign_type='Discador',"
                f"dialer_queue='{self.cola_destino}',"
                f"origination_caller_id_number='{numero}',"
                f"execute_on_answer='transfer {self.cola_destino} XML {self.contexto}'}}"
                f"sofia/gateway/{GATEWAY}/{numero} 2222 XML DETECT_AMD_DIALER"
            )
        else:
            # Sin AMD: transfer directo
            return (
                f"bgapi originate "
                f"{{ignore_early_media=false,"
                f"origination_uuid={uuid},"
                f"campaign_name='{self.campaign_name}',"
                f"campaign_type='Discador',"
                f"dialer_queue='{self.cola_destino}',"
                f"origination_caller_id_number='{numero}',"
                f"execute_on_answer='transfer {self.cola_destino} XML {self.contexto}'}}"
                f"sofia/gateway/{GATEWAY}/{numero} &park()"
            )
    
    async def send_single(self, item: dict) -> tuple:
        """Envía una sola llamada"""
        numero = item.get('telefono', '')
        
        if not numero or not numero.strip().isdigit():
            return (numero, False, {"error": "invalid_number"})
        
        if self.is_active(numero):
            return (numero, False, {"error": "already_active"})
        
        uuid = f"dialer_{self.campaign_name}_{numero}_{int(time.time()*1000000)}"
        
        # Registrar
        self.register_active(numero)
        self.active_uuids.add(uuid)
        
        # Actualizar BD
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(f"""
                    UPDATE `{self.campaign_name}` 
                    SET uuid = :uuid, estado = 'P', fecha_envio = NOW(),
                        intentos = COALESCE(intentos, 0) + 1
                    WHERE telefono = :numero
                    AND estado NOT IN ('C', 'T', 'P', 'R', 'A', 'Q')
                """), {"uuid": uuid, "numero": numero})
                
                if result.rowcount == 0:
                    self.release_active(numero)
                    return (numero, False, {"error": "already_processed"})
        except Exception as e:
            self.release_active(numero)
            return (numero, False, {"error": str(e)})
        
        # Enviar
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
                    return (numero, True, {"uuid": uuid, "queue": self.cola_destino})
                else:
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
        """Envía lote con lógica de overdial"""
        # Obtener agentes disponibles
        await self.get_available_agents()
        
        if self.available_agents < DIALER_MIN_AGENTS:
            self.logger.warning(f"⏳ Solo {self.available_agents} agentes disponibles")
            return []
        
        # Ajustar overdial
        self.adjust_overdial()
        
        # Calcular batch size según agentes y overdial
        batch_size = int(self.available_agents * self.overdial_ratio)
        batch_size = min(batch_size, len(items), self.cps)
        
        actual_batch = items[:batch_size]
        
        results = []
        batch_data = []
        
        for item in actual_batch:
            numero = item.get('telefono', '')
            if not numero or not numero.strip().isdigit():
                results.append((numero, False, {"error": "invalid"}))
                continue
            
            if self.is_active(numero):
                continue
            
            uuid = f"dialer_{self.campaign_name}_{numero}_{int(time.time()*1000000)}"
            originate_str = self._build_originate_string(numero, uuid)
            batch_data.append((numero, uuid, originate_str))
            
            self.register_active(numero)
            self.active_uuids.add(uuid)
        
        # BD batch update
        if batch_data:
            try:
                with self.engine.begin() as conn:
                    for numero, uuid, _ in batch_data:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET uuid = :uuid, estado = 'P', fecha_envio = NOW(),
                                intentos = COALESCE(intentos, 0) + 1
                            WHERE telefono = :numero 
                            AND estado NOT IN ('C', 'T', 'P', 'R', 'A', 'Q')
                        """), {"uuid": uuid, "numero": numero})
            except Exception as e:
                self.logger.error(f"Error batch update: {e}")
        
        # Enviar
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
                if not r[1]:
                    self.release_active(r[0])
            
            if i + MINI_BATCH < len(batch_data):
                await asyncio.sleep(DELAY)
        
        # Stats
        self.stats.extra_data["available_agents"] = self.available_agents
        self.stats.extra_data["total_agents"] = self.total_agents
        
        return results
    
    async def check_status(self, item_id: str) -> dict:
        """Verifica estado"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT estado, hangup_cause, fecha_envio, fecha_transfer,
                           agente_extension, duracion_agente
                    FROM `{self.campaign_name}`
                    WHERE telefono = :numero
                """), {"numero": item_id}).fetchone()
                
                if result:
                    return {
                        "estado": result[0],
                        "hangup_cause": result[1],
                        "fecha_envio": str(result[2]) if result[2] else None,
                        "fecha_transfer": str(result[3]) if result[3] else None,
                        "agente": result[4],
                        "duracion_agente": result[5],
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
                        OR (estado IN ('N', 'O', 'F', 'B', 'E', 'X') 
                            AND (intentos IS NULL OR intentos < :max_retries))
                    )
                    AND estado NOT IN ('C', 'T', 'P', 'R', 'A', 'Q', 'M')
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
