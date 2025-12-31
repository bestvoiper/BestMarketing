"""
Telegram Sender Plugin - Envío de mensajes por Telegram Bot API
"""
import asyncio
import aiohttp
from typing import Optional
from sqlalchemy import text

from .base import BaseSender, SenderStats
from shared_config import get_logger

logger = get_logger("telegram_sender")


class TelegramSender(BaseSender):
    """
    Sender para campañas de Telegram.
    Usa Telegram Bot API para enviar mensajes.
    """
    
    CAMPAIGN_TYPE = "Telegram"
    TERMINAL_STATES = ('delivered', 'X')
    RETRY_STATES = ('failed', 'E')
    
    def __init__(self, campaign_name: str, config: dict = None):
        super().__init__(campaign_name, config)
        
        # Bot config
        self.bot_token = ""
        self.api_url = "https://api.telegram.org"
        self.message_template = ""
        
        # Rate limiting (Telegram: 30 msg/s a grupos, 1 msg/s a usuarios nuevos)
        self.messages_per_second = 25
        
        # Session HTTP
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def initialize(self) -> bool:
        """Inicializa conexión a Telegram Bot API"""
        try:
            config = self.get_campaign_config()
            api_config = config.get("api_config")
            
            if api_config:
                if isinstance(api_config, str):
                    import json
                    api_config = json.loads(api_config)
                
                self.bot_token = api_config.get("bot_token", "")
                self.messages_per_second = api_config.get("mps", 25)
            
            if not self.bot_token:
                self.logger.error("❌ Falta bot_token de Telegram")
                return False
            
            # Template de mensaje
            template = config.get("template")
            if template:
                self.message_template = template
            
            # Crear session HTTP
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
            
            # Verificar bot
            url = f"{self.api_url}/bot{self.bot_token}/getMe"
            async with self.session.get(url) as response:
                result = await response.json()
                if result.get("ok"):
                    bot_info = result.get("result", {})
                    self.logger.info(f"✅ Bot conectado: @{bot_info.get('username')}")
                else:
                    self.logger.error(f"❌ Bot inválido: {result}")
                    return False
            
            self.stats.rate_max = self.messages_per_second
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inicializando Telegram: {e}")
            return False
    
    async def cleanup(self):
        if self.session:
            await self.session.close()
    
    def _get_id_field(self) -> str:
        return 'telefono'  # En Telegram sería chat_id, pero usamos telefono como campo genérico
    
    def _render_message(self, template: str, datos: dict = None) -> str:
        """Renderiza el mensaje con variables"""
        message = template or self.message_template
        if datos:
            for key, value in datos.items():
                message = message.replace(f"{{{key}}}", str(value))
        return message
    
    async def send_single(self, item: dict) -> tuple:
        """Envía un mensaje de Telegram"""
        chat_id = item.get('telefono') or item.get('chat_id', '')
        datos = item.get('datos', {})
        
        if not chat_id:
            return (chat_id, False, {"error": "invalid_chat_id"})
        
        if self.is_active(chat_id):
            return (chat_id, False, {"error": "already_active"})
        
        self.register_active(chat_id)
        
        # Actualizar BD
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                    UPDATE `{self.campaign_name}` 
                    SET estado = 'enviando', fecha_envio = NOW(),
                        intentos = COALESCE(intentos, 0) + 1
                    WHERE telefono = :chat_id AND estado NOT IN ('delivered')
                """), {"chat_id": chat_id})
        except Exception as e:
            self.release_active(chat_id)
            return (chat_id, False, {"error": str(e)})
        
        # Renderizar mensaje
        message_text = self._render_message(self.message_template, datos)
        
        # Enviar
        try:
            url = f"{self.api_url}/bot{self.bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message_text,
                "parse_mode": "HTML"
            }
            
            async with self.session.post(url, json=payload) as response:
                result = await response.json()
                
                if result.get("ok"):
                    message_id = result["result"]["message_id"]
                    
                    with self.engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET estado = 'delivered', message_id = :msg_id,
                                fecha_entrega = NOW()
                            WHERE telefono = :chat_id
                        """), {"msg_id": str(message_id), "chat_id": chat_id})
                    
                    self.release_active(chat_id)
                    return (chat_id, True, {"message_id": message_id})
                else:
                    error = result.get("description", "Unknown error")
                    
                    with self.engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET estado = 'failed', error = :error
                            WHERE telefono = :chat_id
                        """), {"error": error[:200], "chat_id": chat_id})
                    
                    self.release_active(chat_id)
                    return (chat_id, False, {"error": error})
                    
        except Exception as e:
            self.release_active(chat_id)
            return (chat_id, False, {"error": str(e)})
    
    async def send_batch(self, items: list) -> list:
        """Envía lote de mensajes"""
        results = []
        delay = 1.0 / self.messages_per_second
        
        for item in items:
            result = await self.send_single(item)
            results.append(result)
            await asyncio.sleep(delay)
        
        return results
    
    async def check_status(self, item_id: str) -> dict:
        """Verifica estado"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT estado, message_id, fecha_envio, fecha_entrega, error
                    FROM `{self.campaign_name}`
                    WHERE telefono = :chat_id
                """), {"chat_id": item_id}).fetchone()
                
                if result:
                    return {
                        "estado": result[0],
                        "message_id": result[1],
                        "fecha_envio": str(result[2]) if result[2] else None,
                        "fecha_entrega": str(result[3]) if result[3] else None,
                        "error": result[4],
                    }
        except:
            pass
        return {"estado": "unknown"}
