"""
Facebook Messenger Sender Plugin - Envío de mensajes por Facebook Messenger API
"""
import asyncio
import aiohttp
from typing import Optional
from sqlalchemy import text

from .base import BaseSender, SenderStats
from shared_config import get_logger

logger = get_logger("facebook_sender")


class FacebookSender(BaseSender):
    """
    Sender para campañas de Facebook Messenger.
    Usa Meta Messenger API para enviar mensajes.
    """
    
    CAMPAIGN_TYPE = "Facebook"
    TERMINAL_STATES = ('delivered', 'read', 'X')
    RETRY_STATES = ('failed', 'E')
    
    def __init__(self, campaign_name: str, config: dict = None):
        super().__init__(campaign_name, config)
        
        # API config
        self.page_access_token = ""
        self.api_url = "https://graph.facebook.com/v18.0"
        self.page_id = ""
        self.message_template = ""
        
        # Rate limiting (Messenger: ~200 msg/s)
        self.messages_per_second = 50
        
        # Session HTTP
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def initialize(self) -> bool:
        """Inicializa conexión a Messenger API"""
        try:
            config = self.get_campaign_config()
            api_config = config.get("api_config")
            
            if api_config:
                if isinstance(api_config, str):
                    import json
                    api_config = json.loads(api_config)
                
                self.page_access_token = api_config.get("page_access_token", "")
                self.page_id = api_config.get("page_id", "")
                self.messages_per_second = api_config.get("mps", 50)
            
            if not self.page_access_token:
                self.logger.error("❌ Falta page_access_token de Facebook")
                return False
            
            # Template
            template = config.get("template")
            if template:
                self.message_template = template
            
            # Session HTTP
            self.session = aiohttp.ClientSession(
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30)
            )
            
            self.stats.rate_max = self.messages_per_second
            self.logger.info(f"✅ Facebook Messenger API inicializada")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inicializando Facebook: {e}")
            return False
    
    async def cleanup(self):
        if self.session:
            await self.session.close()
    
    def _get_id_field(self) -> str:
        return 'telefono'  # PSID en Facebook, usamos telefono como campo genérico
    
    def _render_message(self, template: str, datos: dict = None) -> str:
        """Renderiza mensaje con variables"""
        message = template or self.message_template
        if datos:
            for key, value in datos.items():
                message = message.replace(f"{{{key}}}", str(value))
        return message
    
    async def send_single(self, item: dict) -> tuple:
        """Envía un mensaje de Messenger"""
        psid = item.get('telefono') or item.get('psid', '')  # Page Scoped ID
        datos = item.get('datos', {})
        
        if not psid:
            return (psid, False, {"error": "invalid_psid"})
        
        if self.is_active(psid):
            return (psid, False, {"error": "already_active"})
        
        self.register_active(psid)
        
        # Actualizar BD
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                    UPDATE `{self.campaign_name}` 
                    SET estado = 'enviando', fecha_envio = NOW(),
                        intentos = COALESCE(intentos, 0) + 1
                    WHERE telefono = :psid AND estado NOT IN ('delivered', 'read')
                """), {"psid": psid})
        except Exception as e:
            self.release_active(psid)
            return (psid, False, {"error": str(e)})
        
        # Mensaje
        message_text = self._render_message(self.message_template, datos)
        
        # Enviar
        try:
            url = f"{self.api_url}/me/messages?access_token={self.page_access_token}"
            payload = {
                "recipient": {"id": psid},
                "message": {"text": message_text},
                "messaging_type": "MESSAGE_TAG",
                "tag": "ACCOUNT_UPDATE"  # Para mensajes fuera de ventana 24h
            }
            
            async with self.session.post(url, json=payload) as response:
                result = await response.json()
                
                if "message_id" in result:
                    message_id = result["message_id"]
                    
                    with self.engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET estado = 'sent', message_id = :msg_id
                            WHERE telefono = :psid
                        """), {"msg_id": message_id, "psid": psid})
                    
                    self.release_active(psid)
                    return (psid, True, {"message_id": message_id})
                else:
                    error = result.get("error", {}).get("message", "Unknown error")
                    
                    with self.engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET estado = 'failed', error = :error
                            WHERE telefono = :psid
                        """), {"error": error[:200], "psid": psid})
                    
                    self.release_active(psid)
                    return (psid, False, {"error": error})
                    
        except Exception as e:
            self.release_active(psid)
            return (psid, False, {"error": str(e)})
    
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
                    WHERE telefono = :psid
                """), {"psid": item_id}).fetchone()
                
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
    
    async def process_webhook(self, webhook_data: dict):
        """Procesa webhooks de Facebook para actualizar estados"""
        try:
            entry = webhook_data.get("entry", [{}])[0]
            messaging = entry.get("messaging", [{}])[0]
            
            # Delivery receipt
            if "delivery" in messaging:
                delivery = messaging["delivery"]
                mids = delivery.get("mids", [])
                for mid in mids:
                    with self.engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET estado = 'delivered', fecha_entrega = NOW()
                            WHERE message_id = :msg_id
                        """), {"msg_id": mid})
            
            # Read receipt
            if "read" in messaging:
                read = messaging["read"]
                watermark = read.get("watermark")
                sender_id = messaging.get("sender", {}).get("id")
                if sender_id:
                    with self.engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET estado = 'read'
                            WHERE telefono = :psid AND estado = 'delivered'
                        """), {"psid": sender_id})
                        
        except Exception as e:
            self.logger.error(f"Error procesando webhook: {e}")
