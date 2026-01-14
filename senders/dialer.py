"""
Dialer Sender Plugin - Marcador predictivo con FreeSWITCH
Origina llamadas y las transfiere a colas de agentes.

Integra con DialerClient para decisiones inteligentes basadas en:
- Estado real de agentes (via WebSocket/AMI)
- Tasa de abandono en tiempo real
- Recomendaciones predictivas del servidor de colas
- Sobrediscado adaptativo
"""
import asyncio
import time
import threading
from typing import Dict, Set, Tuple, Optional
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

# Importar DialerClient para información inteligente de colas
try:
    import sys
    from pathlib import Path
    # Agregar directorio discador al path
    discador_path = Path(__file__).parent.parent / "discador"
    if str(discador_path) not in sys.path:
        sys.path.insert(0, str(discador_path))
    from dialer_client import DialerClient
    DIALER_CLIENT_AVAILABLE = True
except ImportError as e:
    DIALER_CLIENT_AVAILABLE = False
    DialerClient = None

# ESL
try:
    import ESL
    ESL_AVAILABLE = True
except ImportError:
    ESL_AVAILABLE = False

logger = get_logger("dialer_sender")

# Configuración WebSocket para DialerClient
DIALER_WS_URL = "ws://tvnovedades.bestvoiper.com:8766"

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
    
    Integra con DialerClient para:
    - Obtener estado real de agentes via WebSocket
    - Respetar recomendaciones de marcado (stop/pause/slow/proceed/accelerate)
    - Ajustar rate dinámicamente según suggested_rate_per_min
    - Usar sobrediscado inteligente basado en datos reales
    """
    
    CAMPAIGN_TYPE = "Discador"
    TERMINAL_STATES = DIALER_TERMINAL_STATES
    RETRY_STATES = ('N', 'O', 'F', 'E', 'X')
    
    def __init__(self, campaign_name: str, config: dict = None):
        super().__init__(campaign_name, config)
        self.esl_connection = None
        self.esl_executor = ThreadPoolExecutor(max_workers=20, thread_name_prefix="dialer_esl")
        
        # DialerClient para información inteligente
        self.dialer_client = None  # type: Optional[DialerClient]
        self.use_intelligent_dialing = DIALER_CLIENT_AVAILABLE
        self.last_queue_status: Optional[Dict] = None
        self.status_update_interval = 2.0  # segundos entre consultas al servidor
        self._status_task: Optional[asyncio.Task] = None
        
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
        
        # Métricas de agentes (actualizadas por DialerClient)
        self.available_agents = 0
        self.total_agents = 0
        self.abandon_count = 0
        self.answered_count = 0
        
        # Estado del discador inteligente
        self.can_dial = True
        self.dial_recommendation = "proceed"
        self.suggested_rate = 1  # llamadas por minuto
        self.dial_capacity = 0
        self.overdial_extra_calls = 0
        
        # Parámetros de sobrediscado (de BD)
        self.queue_relationated = DIALER_DEFAULT_QUEUE  # Cola para monitoreo AMI
        self.overdial_after_seconds = 5   # Segundos libres antes de sobrediscar
        self.overdial_percent = 50        # Porcentaje de sobrediscado
        self.overdial_multiplier = 2      # Multiplicador de sobrediscado
        self.max_channels = 20            # Canales máximos
    
    async def initialize(self) -> bool:
        """Inicializa conexión ESL y DialerClient"""
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
            
            # Obtener configuración completa de discador desde BD
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text("""
                        SELECT 
                            overdial_ratio,
                            queue_relationated,
                            overdial_after_seconds,
                            overdial_percent,
                            overdial_multiplier,
                            max_channels
                        FROM campanas WHERE nombre = :nombre
                    """), {"nombre": self.campaign_name}).fetchone()
                    
                    if result:
                        if result[0]:
                            self.overdial_ratio = float(result[0])
                        if result[1]:
                            self.queue_relationated = result[1]  # Cola para monitoreo AMI
                        if result[2] is not None:
                            self.overdial_after_seconds = int(result[2])
                        if result[3] is not None:
                            self.overdial_percent = int(result[3])
                        if result[4] is not None:
                            self.overdial_multiplier = int(result[4])
                        if result[5] is not None:
                            self.max_channels = int(result[5])
            except Exception as e:
                self.logger.warning(f"⚠️ Error obteniendo config de BD: {e}")
            
            self.logger.info(f"📞 Cola destino: {self.cola_destino} | Cola AMI: {self.queue_relationated}")
            self.logger.info(f"📊 Overdial: ratio={self.overdial_ratio} after={self.overdial_after_seconds}s")
            self.logger.info(f"📊 Overdial: percent={self.overdial_percent}% multiplier={self.overdial_multiplier}")
            self.logger.info(f"📡 Max channels: {self.max_channels} | CPS: {self.cps}")
            
            # Inicializar DialerClient para información inteligente
            if self.use_intelligent_dialing and DIALER_CLIENT_AVAILABLE:
                await self._initialize_dialer_client()
            
            self.stats.rate_max = self.cps
            self.stats.extra_data["overdial_ratio"] = self.overdial_ratio
            self.stats.extra_data["cola_destino"] = self.cola_destino
            self.stats.extra_data["queue_relationated"] = self.queue_relationated
            self.stats.extra_data["overdial_after_seconds"] = self.overdial_after_seconds
            self.stats.extra_data["overdial_percent"] = self.overdial_percent
            self.stats.extra_data["overdial_multiplier"] = self.overdial_multiplier
            self.stats.extra_data["max_channels"] = self.max_channels
            self.stats.extra_data["intelligent_dialing"] = self.use_intelligent_dialing
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inicializando: {e}")
            return False
    
    async def _initialize_dialer_client(self):
        """Inicializa el cliente WebSocket para información de colas"""
        try:
            self.logger.info(f"🔌 Conectando a servidor de colas: {DIALER_WS_URL}")
            # Usar queue_relationated para monitoreo AMI (no cola_destino)
            self.dialer_client = DialerClient(DIALER_WS_URL, self.queue_relationated)
            
            # Configurar callback para cambios de estado
            def on_status_update(status: Dict):
                self._process_queue_status(status)
            
            def on_can_dial_change(can_dial: bool, reason: str):
                self.can_dial = can_dial
                if can_dial:
                    self.logger.info(f"🟢 MARCADO HABILITADO: {reason}")
                else:
                    self.logger.warning(f"🔴 MARCADO PAUSADO: {reason}")
            
            self.dialer_client.on_status_update = on_status_update
            self.dialer_client.on_can_dial_change = on_can_dial_change
            
            # Conectar
            if await self.dialer_client.connect():
                self.logger.info(f"✅ DialerClient conectado a cola: {self.queue_relationated}")
                
                # Configurar parámetros del discador en el servidor
                # Equivalente a: --overdial-after 5 --overdial-percent 50 --overdial-multiplier 2 --max-channels 20
                await self.dialer_client.configure_dialer({
                    "max_ratio": 3,
                    "min_agents": DIALER_MIN_AGENTS,
                    "max_wait_calls": 5,
                    "pause_on_abandon_rate": DIALER_ABANDON_THRESHOLD,
                    # Parámetros de SobreDiscado (de BD)
                    "overdial_after_seconds": self.overdial_after_seconds,
                    "overdial_percent": self.overdial_percent,
                    "overdial_multiplier": self.overdial_multiplier,
                    "max_channels": self.max_channels
                })
                
                self.logger.info(
                    f"⚙️ Config enviada: overdial_after={self.overdial_after_seconds}s "
                    f"percent={self.overdial_percent}% multiplier={self.overdial_multiplier} "
                    f"max_channels={self.max_channels}"
                )
                
                # Iniciar task de actualización de estado
                self._status_task = asyncio.create_task(self._status_update_loop())
                
                # Obtener estado inicial
                status = await self.dialer_client.get_status()
                if status:
                    self._process_queue_status(status)
            else:
                self.logger.warning("⚠️ No se pudo conectar DialerClient - usando modo básico")
                self.use_intelligent_dialing = False
                
        except Exception as e:
            self.logger.warning(f"⚠️ Error inicializando DialerClient: {e} - usando modo básico")
            self.use_intelligent_dialing = False
    
    def _process_queue_status(self, status: Dict):
        """Procesa el estado de la cola y actualiza variables internas"""
        if not status or not status.get("found"):
            return
        
        self.last_queue_status = status
        
        # Extraer información de agentes
        agents = status.get("agents", {})
        self.available_agents = agents.get("available", 0)
        self.total_agents = agents.get("total", 0)
        
        # Extraer información de llamadas
        calls = status.get("calls", {})
        abandon_rate = calls.get("abandon_rate", 0)
        
        # Extraer recomendaciones del discador
        dialer = status.get("dialer", {})
        self.can_dial = dialer.get("can_dial", True)
        self.dial_recommendation = dialer.get("recommendation", "proceed")
        self.suggested_rate = dialer.get("suggested_rate_per_min", 1)
        self.dial_capacity = dialer.get("dial_capacity", 0)
        
        # Extraer información de sobrediscado
        overdial = status.get("overdial", {})
        if overdial.get("enabled"):
            self.overdial_extra_calls = overdial.get("extra_calls", 0)
        else:
            self.overdial_extra_calls = 0
        
        # Actualizar stats para visualización
        self.stats.extra_data.update({
            "available_agents": self.available_agents,
            "total_agents": self.total_agents,
            "abandon_rate": abandon_rate,
            "can_dial": self.can_dial,
            "recommendation": self.dial_recommendation,
            "suggested_rate": self.suggested_rate,
            "dial_capacity": self.dial_capacity,
            "overdial_extra": self.overdial_extra_calls
        })
    
    async def _status_update_loop(self):
        """Loop que actualiza el estado de la cola periódicamente"""
        while self._running:
            try:
                if self.dialer_client and self.dialer_client.connected:
                    status = await self.dialer_client.get_status()
                    if status:
                        self._process_queue_status(status)
                await asyncio.sleep(self.status_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.debug(f"Error en status loop: {e}")
                await asyncio.sleep(5)
    
    async def cleanup(self):
        """Limpia recursos"""
        try:
            # Detener task de status
            if self._status_task:
                self._status_task.cancel()
                try:
                    await self._status_task
                except asyncio.CancelledError:
                    pass
            
            # Desconectar DialerClient
            if self.dialer_client:
                await self.dialer_client.disconnect()
            
            # Desconectar ESL
            if self.esl_connection and self.esl_connection.connected():
                self.esl_connection.disconnect()
            
            self.esl_executor.shutdown(wait=False)
        except:
            pass
    
    async def get_available_agents(self) -> dict:
        """
        Obtiene agentes disponibles para la cola.
        
        Si DialerClient está conectado, usa información en tiempo real.
        Si no, consulta la tabla de agentes como fallback.
        """
        # Si tenemos información actualizada del DialerClient, usarla
        if self.use_intelligent_dialing and self.last_queue_status:
            agents = self.last_queue_status.get("agents", {})
            self.total_agents = agents.get("total", 0)
            self.available_agents = agents.get("available", 0)
            return {
                "total": self.total_agents,
                "available": self.available_agents,
                "busy": agents.get("busy", 0),
                "paused": agents.get("paused", 0),
                "source": "dialer_client"
            }
        
        # Fallback: consultar BD
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
                        "busy": result[2] or 0,
                        "source": "database"
                    }
        except:
            pass
        
        # Default si no hay información
        self.total_agents = 5
        self.available_agents = 3
        return {"total": 5, "available": 3, "busy": 2, "source": "default"}
    
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
        """
        Envía lote con lógica inteligente de marcado.
        
        Si DialerClient está conectado:
        - Respeta can_dial (no marca si es False)
        - Ajusta batch_size según dial_capacity
        - Usa suggested_rate para el timing
        - Considera overdial_extra_calls
        
        Si no, usa lógica tradicional de overdial.
        """
        # Obtener estado actual de agentes
        await self.get_available_agents()
        
        # === MODO INTELIGENTE ===
        if self.use_intelligent_dialing and self.last_queue_status:
            # Verificar si podemos marcar
            if not self.can_dial:
                self.logger.warning(f"⏸️ Marcado pausado: {self.dial_recommendation}")
                self.stats.extra_data["paused_reason"] = self.dial_recommendation
                return []
            
            # Verificar recomendación
            if self.dial_recommendation == "stop":
                self.logger.warning("🛑 Recomendación: STOP - No marcar")
                return []
            
            if self.dial_recommendation == "pause":
                self.logger.info("⏸️ Recomendación: PAUSE - Esperando...")
                await asyncio.sleep(5)  # Esperar antes de reintentar
                return []
            
            # Calcular batch_size inteligente
            batch_size = self.dial_capacity
            
            # Agregar llamadas extra de sobrediscado si aplica
            if self.overdial_extra_calls > 0:
                batch_size += self.overdial_extra_calls
                self.logger.debug(f"📈 Sobrediscado: +{self.overdial_extra_calls} llamadas")
            
            # Ajustar según recomendación
            if self.dial_recommendation == "slow":
                batch_size = max(1, batch_size // 2)
                self.logger.debug(f"🐢 Modo lento: batch reducido a {batch_size}")
            elif self.dial_recommendation == "accelerate":
                batch_size = int(batch_size * 1.5)
                self.logger.debug(f"🚀 Acelerando: batch aumentado a {batch_size}")
            
            # Limitar al máximo configurado
            batch_size = min(batch_size, len(items), self.max_concurrent)
            
            if batch_size <= 0:
                self.logger.info("📋 Sin capacidad de marcado disponible")
                return []
            
            self.logger.info(
                f"🎯 Marcado inteligente: {batch_size} llamadas "
                f"(cap: {self.dial_capacity}, rec: {self.dial_recommendation})"
            )
        
        # === MODO TRADICIONAL ===
        else:
            if self.available_agents < DIALER_MIN_AGENTS:
                self.logger.warning(f"⏳ Solo {self.available_agents} agentes disponibles")
                return []
            
            # Ajustar overdial tradicional
            self.adjust_overdial()
            
            # Calcular batch size según agentes y overdial
            batch_size = int(self.available_agents * self.overdial_ratio)
            batch_size = min(batch_size, len(items), self.cps)
            
            self.logger.info(
                f"📞 Marcado tradicional: {batch_size} llamadas "
                f"(agentes: {self.available_agents}, ratio: {self.overdial_ratio})"
            )
        
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
        
        # Enviar llamadas
        loop = asyncio.get_event_loop()
        
        # Calcular delay entre mini-batches según modo
        if self.use_intelligent_dialing and self.suggested_rate > 0:
            # Convertir llamadas/minuto a delay entre llamadas
            calls_per_second = self.suggested_rate / 60.0
            DELAY = max(0.2, 1.0 / calls_per_second) if calls_per_second > 0 else 0.5
        else:
            DELAY = 0.35
        
        MINI_BATCH = 5
        
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
        self.stats.extra_data["batch_sent"] = len(results)
        
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
