#!/usr/bin/env python3
"""
Cliente de ejemplo para el Discador - Conexión WebSocket al monitor de colas

Este cliente se conecta al WebSocket del monitor de colas y obtiene
información en tiempo real para tomar decisiones de marcado.

Uso:
    python dialer_client.py --queue ventas
    python dialer_client.py --queue soporte --ws-url ws://servidor:8766
"""

import asyncio
import json
import argparse
import logging
from datetime import datetime
from typing import Optional, Callable, Dict, Any

try:
    import websockets
except ImportError:
    print("❌ Instala websockets: pip install websockets")
    exit(1)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class DialerClient:
    """
    Cliente WebSocket para el discador.
    Se conecta al monitor de colas y obtiene información para decisiones de marcado.
    """
    
    def __init__(self, ws_url: str, queue_name: str):
        self.ws_url = ws_url
        self.queue_name = queue_name
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.connected = False
        self.running = False
        self.last_status: Optional[Dict] = None
        self.last_queue_update: Optional[Dict] = None
        
        # Cola de respuestas pendientes
        self.pending_responses: Dict[str, asyncio.Queue] = {}
        self.message_listener_task: Optional[asyncio.Task] = None
        
        # Callbacks para eventos
        self.on_status_update: Optional[Callable[[Dict], None]] = None
        self.on_can_dial_change: Optional[Callable[[bool, str], None]] = None
        self.on_queue_update: Optional[Callable[[Dict], None]] = None
        
        # Estado del discador
        self.can_dial = False
        self.dial_rate = 0
        self.dial_recommendation = "stop"
    
    async def connect(self) -> bool:
        """Conecta al servidor WebSocket"""
        try:
            logger.info(f"🔌 Conectando a {self.ws_url}...")
            self.ws = await websockets.connect(
                self.ws_url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10
            )
            self.connected = True
            logger.info("✅ Conectado al servidor de colas")
            
            # Iniciar listener de mensajes en background
            self.message_listener_task = asyncio.create_task(self._message_listener())
            
            return True
        except Exception as e:
            logger.error(f"❌ Error conectando: {e}")
            return False
    
    async def _message_listener(self):
        """Listener de mensajes en background"""
        try:
            async for message in self.ws:
                try:
                    data = json.loads(message)
                    msg_type = data.get("type", "")
                    
                    # Si hay alguien esperando este tipo de mensaje
                    if msg_type in self.pending_responses:
                        await self.pending_responses[msg_type].put(data)
                    
                    # Procesar según tipo
                    if msg_type == "initial_data":
                        logger.info(f"📊 Datos iniciales recibidos: {data.get('data', {}).get('total_queues', 0)} colas")
                        self.last_queue_update = data.get("data", {})
                    
                    elif msg_type == "queue_update":
                        self.last_queue_update = data.get("data", {})
                        if self.on_queue_update:
                            self.on_queue_update(self.last_queue_update)
                    
                    elif msg_type == "dialer_status":
                        self.last_status = data.get("data", {})
                        self._process_status(self.last_status)
                    
                    elif msg_type == "dialer_config_updated":
                        logger.info(f"⚙️ Configuración actualizada: {data.get('data', {})}")
                    
                    elif msg_type == "error":
                        logger.error(f"❌ Error del servidor: {data.get('message')}")
                    
                    elif msg_type == "pong":
                        pass  # Silencioso
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️ Mensaje no JSON: {message[:100]}")
                except Exception as e:
                    logger.error(f"❌ Error procesando mensaje: {e}")
                    
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"🔌 Conexión cerrada: {e}")
            self.connected = False
        except Exception as e:
            logger.error(f"❌ Error en listener: {e}")
            self.connected = False
    
    async def disconnect(self):
        """Desconecta del servidor"""
        self.running = False
        
        if self.message_listener_task:
            self.message_listener_task.cancel()
            try:
                await self.message_listener_task
            except asyncio.CancelledError:
                pass
        
        if self.ws:
            await self.ws.close()
        self.connected = False
        logger.info("🔌 Desconectado")
    
    async def configure_dialer(self, config: Dict) -> bool:
        """
        Configura parámetros del discador en el servidor.
        
        Parámetros disponibles:
        - max_ratio: Máximo ratio llamadas/agente (default: 3)
        - min_agents: Mínimo agentes para marcar (default: 1)
        - max_wait_calls: Máx llamadas en espera permitidas (default: 5)
        - pause_on_abandon_rate: % de abandono para pausar (default: 15)
        """
        if not self.connected:
            return False
        
        try:
            await self.ws.send(json.dumps({
                "type": "dialer_config",
                "queue_name": self.queue_name,
                "config": config
            }))
            logger.info(f"⚙️ Configuración enviada: {config}")
            return True
        except Exception as e:
            logger.error(f"❌ Error enviando config: {e}")
            return False
    
    async def request_status(self) -> bool:
        """Solicita el estado de la cola (la respuesta llega por el listener)"""
        if not self.connected:
            return False
        
        try:
            await self.ws.send(json.dumps({
                "type": "dialer_status",
                "queue_name": self.queue_name
            }))
            return True
        except Exception as e:
            logger.error(f"❌ Error solicitando status: {e}")
            return False
    
    async def get_status(self) -> Optional[Dict]:
        """Obtiene el estado actual de la cola para el discador"""
        if not self.connected:
            return None
        
        try:
            # Crear cola para esperar respuesta
            self.pending_responses["dialer_status"] = asyncio.Queue()
            
            # Enviar solicitud
            await self.ws.send(json.dumps({
                "type": "dialer_status",
                "queue_name": self.queue_name
            }))
            
            # Esperar respuesta con timeout
            try:
                response = await asyncio.wait_for(
                    self.pending_responses["dialer_status"].get(), 
                    timeout=10
                )
                return response.get("data", {})
            except asyncio.TimeoutError:
                logger.warning("⏱️ Timeout esperando dialer_status")
                return self.last_status
            finally:
                del self.pending_responses["dialer_status"]
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo status: {e}")
            return None
    
    def _process_status(self, status: Dict):
        """Procesa el estado recibido y actualiza variables internas"""
        if not status.get("found"):
            logger.warning(f"⚠️ Cola no encontrada: {status.get('error')}")
            self.can_dial = False
            self.dial_rate = 0
            self.dial_recommendation = "stop"
            return
        
        dialer_info = status.get("dialer", {})
        old_can_dial = self.can_dial
        
        self.can_dial = dialer_info.get("can_dial", False)
        self.dial_rate = dialer_info.get("suggested_rate_per_min", 0)
        self.dial_recommendation = dialer_info.get("recommendation", "stop")
        
        # Callback si cambió el estado de marcado
        if self.on_can_dial_change and old_can_dial != self.can_dial:
            self.on_can_dial_change(self.can_dial, dialer_info.get("reason", ""))
        
        # Callback de actualización
        if self.on_status_update:
            self.on_status_update(status)
    
    async def monitor_loop(self, interval: float = 2.0):
        """
        Loop de monitoreo continuo.
        Consulta el estado cada 'interval' segundos.
        """
        self.running = True
        logger.info(f"🔄 Iniciando monitoreo de cola '{self.queue_name}' cada {interval}s")
        
        # Esperar un momento para recibir datos iniciales
        await asyncio.sleep(1)
        
        while self.running and self.connected:
            try:
                status = await self.get_status()
                
                if status:
                    self._print_status(status)
                elif self.last_status:
                    # Usar último estado conocido
                    self._print_status(self.last_status)
                else:
                    logger.warning("⚠️ Sin datos de estado disponibles")
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"❌ Error en loop: {e}")
                await asyncio.sleep(interval)
    
    def _print_status(self, status: Dict):
        """Imprime el estado de forma legible"""
        if not status.get("found"):
            print(f"\n❌ Cola '{self.queue_name}' no encontrada")
            return
        
        agents = status.get("agents", {})
        agents_by_status = status.get("agents_by_status", {})
        calls = status.get("calls", {})
        dialer = status.get("dialer", {})
        prediction = status.get("prediction")
        
        # Colores según recomendación
        rec = dialer.get("recommendation", "stop")
        if rec == "stop":
            color = "\033[91m"  # Rojo
            icon = "🔴"
        elif rec == "pause":
            color = "\033[93m"  # Amarillo
            icon = "🟡"
        elif rec == "slow":
            color = "\033[93m"  # Amarillo
            icon = "🟠"
        elif rec == "accelerate":
            color = "\033[92m"  # Verde brillante
            icon = "🟢"
        else:  # proceed
            color = "\033[92m"  # Verde
            icon = "🟢"
        
        reset = "\033[0m"
        
        print(f"\n{'='*70}")
        print(f"📞 DIALER STATUS - {self.queue_name} | {status.get('timestamp', '')[:19]}")
        print(f"{'='*70}")
        
        print(f"\n👥 AGENTES (Resumen):")
        print(f"   ✅ Disponibles: {agents.get('available', 0)}")
        print(f"   📞 Ocupados: {agents.get('busy', 0)}")
        print(f"   ⏸️  Pausados: {agents.get('paused', 0)}")
        print(f"   📊 Total registrados: {agents.get('total', 0)}")
        
        # Mostrar desglose por estado si está disponible
        if agents_by_status:
            paused_by_status = status.get("paused_by_status", {})
            
            print(f"\n👥 AGENTES (Desglose por Estado):")
            print(f"   ┌──────────────────────────────────────────────────────────────────────────┐")
            print(f"   │ Estado                    │ Código │ Total │ Pausados │ Activos Reales │")
            print(f"   ├──────────────────────────────────────────────────────────────────────────┤")
            
            status_labels = [
                ("not_in_use", "🟢 No en uso (disponible)", 1),
                ("in_use", "📞 En uso (en llamada)", 2),
                ("busy", "🔴 Ocupado", 3),
                ("ringing", "🔔 Timbrando", 6),
                ("ringing_in_use", "🔔 Timbrando en uso", 7),
                ("on_hold", "⏸️  En espera (hold)", 8),
                ("unavailable", "⚫ No disponible", 5),
                ("invalid", "❌ Inválido", 4),
                ("unknown", "❓ Desconocido", 0)
            ]
            
            for status_key, label, code in status_labels:
                count = agents_by_status.get(status_key, 0)
                paused = paused_by_status.get(status_key, 0)
                activos = count - paused
                paused_str = f"{paused}" if paused > 0 else "-"
                activos_str = f"{activos}" if count > 0 else "-"
                print(f"   │ {label:<25} │   {code:<4} │  {count:>3}  │    {paused_str:>3}   │       {activos_str:>3}      │")
            
            print(f"   └──────────────────────────────────────────────────────────────────────────┘")
            
            # Nota explicativa
            print(f"\n   ℹ️  Disponibles = 'No en uso' - Pausados en ese estado")
            print(f"   ℹ️  En este caso: {agents_by_status.get('not_in_use', 0)} 'No en uso' - {paused_by_status.get('not_in_use', 0)} pausados = {agents_by_status.get('not_in_use', 0) - paused_by_status.get('not_in_use', 0)} disponibles")
            
            # Resumen de activos vs inactivos
            activos = (agents_by_status.get("not_in_use", 0) + 
                      agents_by_status.get("in_use", 0) + 
                      agents_by_status.get("ringing", 0) +
                      agents_by_status.get("ringing_in_use", 0) +
                      agents_by_status.get("on_hold", 0))
            inactivos = (agents_by_status.get("unavailable", 0) + 
                        agents_by_status.get("invalid", 0) + 
                        agents_by_status.get("unknown", 0))
            total_pausados = agents.get('paused', 0)
            
            print(f"\n   📈 Conectados: {activos} | ⚫ Desconectados: {inactivos} | ⏸️ Total Pausados: {total_pausados}")
        
        print(f"\n📞 LLAMADAS:")
        print(f"   ⏳ En espera: {calls.get('waiting', 0)}")
        print(f"   ⏱️  Tiempo espera: {calls.get('hold_time_avg', 0)}s")
        print(f"   ✅ Completadas: {calls.get('completed', 0)}")
        print(f"   ❌ Abandonadas: {calls.get('abandoned', 0)} ({calls.get('abandon_rate', 0)}%)")
        
        print(f"\n{color}🎯 DISCADOR:{reset}")
        print(f"   {icon} Puede marcar: {'SÍ' if dialer.get('can_dial') else 'NO'}")
        print(f"   📋 Recomendación: {dialer.get('recommendation', 'N/A').upper()}")
        print(f"   💬 Razón: {dialer.get('reason', 'N/A')}")
        print(f"   📈 Capacidad: {dialer.get('dial_capacity', 0)} llamadas")
        print(f"   ⚡ Velocidad sugerida: {dialer.get('suggested_rate_per_min', 0)} llamadas/min")
        
        # Mostrar información de SobreDiscado
        overdial = status.get("overdial", {})
        if overdial.get("enabled"):
            print(f"\n📞 SOBREDISCADO:")
            print(f"   ⏱️  Sobrediscar después de: {overdial.get('after_seconds', 0)}s libres")
            print(f"   📊 Porcentaje: {overdial.get('percent', 0)}% x {overdial.get('multiplier', 1)}")
            print(f"   👥 Agentes elegibles: {overdial.get('eligible_agents', 0)}")
            print(f"   ➕ Llamadas extra: {overdial.get('extra_calls', 0)}")
            if overdial.get('max_channels', 0) > 0:
                print(f"   📡 Canales: {overdial.get('current_active', 0)}/{overdial.get('max_channels', 0)} (disponibles: {overdial.get('channels_available', 0)})")
        elif overdial.get('max_channels', 0) > 0:
            print(f"\n📡 CANALES MÁXIMOS:")
            print(f"   Activos: {overdial.get('current_active', 0)}/{overdial.get('max_channels', 0)}")
            print(f"   Disponibles: {overdial.get('channels_available', 0)}")
        
        if prediction:
            print(f"\n🔮 PREDICCIÓN (5 min):")
            print(f"   📞 Llamadas esperadas: {prediction.get('calls_in_5min', 'N/A')}")
            print(f"   👥 Agentes esperados: {prediction.get('agents_in_5min', 'N/A')}")
            print(f"   📊 Tendencia: {prediction.get('trend', 'N/A')}")
        
        print(f"\n{'='*70}")


async def main():
    parser = argparse.ArgumentParser(description="Cliente Dialer - Monitor de Colas")
    parser.add_argument("-q", "--queue", required=True, help="Nombre de la cola a monitorear")
    parser.add_argument("-u", "--ws-url", default="ws://tvnovedades.bestvoiper.com:8766", 
                        help="URL del servidor WebSocket (default: ws://tvnovedades.bestvoiper.com:8766)")
    parser.add_argument("-i", "--interval", type=float, default=3.0,
                        help="Intervalo de consulta en segundos (default: 3)")
    parser.add_argument("--max-ratio", type=int, default=3,
                        help="Máximo ratio llamadas/agente (default: 3)")
    parser.add_argument("--min-agents", type=int, default=1,
                        help="Mínimo agentes para marcar (default: 1)")
    parser.add_argument("--max-wait", type=int, default=5,
                        help="Máximo llamadas en espera (default: 5)")
    parser.add_argument("--pause-abandon", type=float, default=15,
                        help="%% abandono para pausar (default: 15)")
    # Parámetros de SobreDiscado
    parser.add_argument("--overdial-after", type=int, default=0,
                        help="SobreDiscar luego de X segundos libres (default: 0 = deshabilitado)")
    parser.add_argument("--overdial-percent", type=int, default=0,
                        help="Porcentaje de sobrediscado (default: 0)")
    parser.add_argument("--overdial-multiplier", type=int, default=1,
                        help="Multiplicador de sobrediscado - aplica de 2da vuelta (default: 1)")
    parser.add_argument("--max-channels", type=int, default=0,
                        help="Canales máximos a utilizar (default: 0 = sin límite)")
    
    args = parser.parse_args()
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   📞 DIALER CLIENT - Monitor de Colas para Marcador         ║
║                                                              ║
║   Cola: {args.queue:<50} ║
║   Servidor: {args.ws_url:<45} ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    client = DialerClient(args.ws_url, args.queue)
    
    # Callback cuando cambia el estado de marcado
    def on_dial_change(can_dial: bool, reason: str):
        if can_dial:
            logger.info(f"🟢 MARCADO HABILITADO: {reason}")
        else:
            logger.warning(f"🔴 MARCADO PAUSADO: {reason}")
    
    client.on_can_dial_change = on_dial_change
    
    try:
        if not await client.connect():
            return
        
        # Configurar parámetros del discador
        await client.configure_dialer({
            "max_ratio": args.max_ratio,
            "min_agents": args.min_agents,
            "max_wait_calls": args.max_wait,
            "pause_on_abandon_rate": args.pause_abandon,
            # Parámetros de SobreDiscado
            "overdial_after_seconds": args.overdial_after,
            "overdial_percent": args.overdial_percent,
            "overdial_multiplier": args.overdial_multiplier,
            "max_channels": args.max_channels
        })
        
        # Iniciar monitoreo
        await client.monitor_loop(interval=args.interval)
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Deteniendo cliente...")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
