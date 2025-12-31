"""
WhatsApp Sender Plugin - Envío de mensajes por WhatsApp Business API
"""
import asyncio
import aiohttp
import time
from typing import Dict, Optional
from sqlalchemy import text

from .base import BaseSender, SenderStats, REDIS_AVAILABLE, redis_client
from shared_config import get_logger

logger = get_logger("whatsapp_sender")


class WhatsAppSender(BaseSender):
    """
    Sender para campañas de WhatsApp.
    Usa WhatsApp Business API (Meta) o proveedores como Twilio, 360dialog, etc.
    """
    
    CAMPAIGN_TYPE = "WhatsApp"
    TERMINAL_STATES = ('delivered', 'read', 'failed', 'X')
    RETRY_STATES = ('failed', 'expired', 'E')
    
    def __init__(self, campaign_name: str, config: dict = None):
        super().__init__(campaign_name, config)
        
        # API Config
        self.api_url = ""
        self.api_token = ""
        self.phone_number_id = ""
        self.template_name = ""
        self.template_language = "es"
        
        # Rate limiting
        self.messages_per_second = 10
        self.daily_limit = 1000
        
        # Session HTTP
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def initialize(self) -> bool:
        """Inicializa conexión a API de WhatsApp"""
        try:
            # Obtener configuración de API desde BD
            config = self.get_campaign_config()
            api_config = config.get("api_config")
            
            if api_config:
                if isinstance(api_config, str):
                    import json
                    api_config = json.loads(api_config)
                
                self.api_url = api_config.get("api_url", "https://graph.facebook.com/v18.0")
                self.api_token = api_config.get("api_token", "")
                self.phone_number_id = api_config.get("phone_number_id", "")
                self.template_name = api_config.get("template_name", "")
                self.template_language = api_config.get("template_language", "es")
                self.messages_per_second = api_config.get("mps", 10)
            
            if not self.api_token or not self.phone_number_id:
                self.logger.error("❌ Falta configuración de WhatsApp API")
                return False
            
            # Crear session HTTP
            self.session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.api_token}",
                    "Content-Type": "application/json"
                },
                timeout=aiohttp.ClientTimeout(total=30)
            )
            
            # Obtener template
            template = config.get("template")
            if template:
                self.message_template = template
            
            self.stats.rate_max = self.messages_per_second
            self.logger.info(f"✅ WhatsApp API inicializada (MPS: {self.messages_per_second})")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inicializando WhatsApp: {e}")
            return False
    
    async def cleanup(self):
        """Cierra session HTTP"""
        if self.session:
            await self.session.close()
    
    def _get_id_field(self) -> str:
        return 'telefono'
    
    def _build_message_payload(self, numero: str, datos: dict = None) -> dict:
        """Construye el payload del mensaje"""
        # Formatear número (agregar código país si falta)
        if not numero.startswith('+'):
            numero = f"+{numero}"
        
        payload = {
            "messaging_product": "whatsapp",
            "to": numero,
            "type": "template",
            "template": {
                "name": self.template_name,
                "language": {"code": self.template_language}
            }
        }
        
        # Si hay variables en el template
        if datos:
            components = []
            if "variables" in datos:
                body_params = [{"type": "text", "text": v} for v in datos["variables"]]
                components.append({"type": "body", "parameters": body_params})
            payload["template"]["components"] = components
        
        return payload
    
    async def send_single(self, item: dict) -> tuple:
        """Envía un mensaje de WhatsApp"""
        numero = item.get('telefono', '')
        datos = item.get('datos')
        
        if not numero:
            return (numero, False, {"error": "invalid_number"})
        
        if self.is_active(numero):
            return (numero, False, {"error": "already_active"})
        
        self.register_active(numero)
        
        # Actualizar BD
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                    UPDATE `{self.campaign_name}` 
                    SET estado = 'enviando', fecha_envio = NOW(),
                        intentos = COALESCE(intentos, 0) + 1
                    WHERE telefono = :numero AND estado NOT IN ('delivered', 'read')
                """), {"numero": numero})
        except Exception as e:
            self.release_active(numero)
            return (numero, False, {"error": str(e)})
        
        # Enviar mensaje
        try:
            payload = self._build_message_payload(numero, datos)
            url = f"{self.api_url}/{self.phone_number_id}/messages"
            
            async with self.session.post(url, json=payload) as response:
                result = await response.json()
                
                if response.status == 200 and "messages" in result:
                    message_id = result["messages"][0]["id"]
                    
                    # Actualizar BD con mensaje enviado
                    with self.engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET estado = 'sent', message_id = :msg_id
                            WHERE telefono = :numero
                        """), {"msg_id": message_id, "numero": numero})
                    
                    self.release_active(numero)
                    return (numero, True, {"message_id": message_id, "status": "sent"})
                else:
                    error = result.get("error", {}).get("message", "Unknown error")
                    
                    with self.engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET estado = 'failed', error = :error
                            WHERE telefono = :numero
                        """), {"error": error[:200], "numero": numero})
                    
                    self.release_active(numero)
                    return (numero, False, {"error": error})
                    
        except Exception as e:
            self.release_active(numero)
            return (numero, False, {"error": str(e)})
    
    async def send_batch(self, items: list) -> list:
        """Envía lote de mensajes"""
        results = []
        
        # WhatsApp tiene rate limits estrictos, enviar secuencialmente con delay
        delay = 1.0 / self.messages_per_second
        
        for item in items:
            result = await self.send_single(item)
            results.append(result)
            await asyncio.sleep(delay)
        
        return results
    
    async def check_status(self, item_id: str) -> dict:
        """Verifica estado de un mensaje"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT estado, message_id, fecha_envio, fecha_entrega, error
                    FROM `{self.campaign_name}`
                    WHERE telefono = :numero
                """), {"numero": item_id}).fetchone()
                
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
        """Procesa webhooks de WhatsApp para actualizar estados"""
        try:
            entry = webhook_data.get("entry", [{}])[0]
            changes = entry.get("changes", [{}])[0]
            value = changes.get("value", {})
            
            statuses = value.get("statuses", [])
            for status in statuses:
                message_id = status.get("id")
                status_type = status.get("status")  # sent, delivered, read, failed
                
                if message_id and status_type:
                    with self.engine.begin() as conn:
                        conn.execute(text(f"""
                            UPDATE `{self.campaign_name}` 
                            SET estado = :estado, fecha_entrega = NOW()
                            WHERE message_id = :msg_id
                        """), {"estado": status_type, "msg_id": message_id})
                    
                    self.logger.info(f"📱 WhatsApp status: {message_id} -> {status_type}")
        except Exception as e:
            self.logger.error(f"Error procesando webhook: {e}")
