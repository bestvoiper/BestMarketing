"""
SMS Sender Plugin - Envío de SMS masivos
Soporta múltiples proveedores: Twilio, Vonage, AWS SNS, etc.
"""
import asyncio
import aiohttp
from typing import Optional
from sqlalchemy import text

from .base import BaseSender, SenderStats
from shared_config import get_logger

logger = get_logger("sms_sender")


class SMSSender(BaseSender):
    """
    Sender para campañas de SMS.
    Soporta múltiples proveedores de SMS.
    """
    
    CAMPAIGN_TYPE = "SMS"
    TERMINAL_STATES = ('delivered', 'X')
    RETRY_STATES = ('failed', 'undelivered', 'E')
    
    # Proveedores soportados
    PROVIDERS = ['twilio', 'vonage', 'aws_sns', 'generic']
    
    def __init__(self, campaign_name: str, config: dict = None):
        super().__init__(campaign_name, config)
        
        # Provider config
        self.provider = "twilio"
        self.api_url = ""
        self.api_key = ""
        self.api_secret = ""
        self.from_number = ""
        self.message_template = ""
        
        # Rate limiting
        self.messages_per_second = 10
        
        # Session HTTP
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def initialize(self) -> bool:
        """Inicializa conexión al proveedor SMS"""
        try:
            config = self.get_campaign_config()
            api_config = config.get("api_config")
            
            if api_config:
                if isinstance(api_config, str):
                    import json
                    api_config = json.loads(api_config)
                
                self.provider = api_config.get("provider", "twilio")
                self.api_key = api_config.get("api_key", "")
                self.api_secret = api_config.get("api_secret", "")
                self.from_number = api_config.get("from_number", "")
                self.messages_per_second = api_config.get("mps", 10)
                
                # URLs por proveedor
                if self.provider == "twilio":
                    account_sid = api_config.get("account_sid", self.api_key)
                    self.api_url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
                elif self.provider == "vonage":
                    self.api_url = "https://rest.nexmo.com/sms/json"
                elif self.provider == "generic":
                    self.api_url = api_config.get("api_url", "")
            
            if not self.api_key:
                self.logger.error(f"❌ Falta api_key para {self.provider}")
                return False
            
            # Template
            template = config.get("template")
            if template:
                self.message_template = template
            
            # Session HTTP con auth básica para Twilio
            auth = None
            if self.provider == "twilio":
                auth = aiohttp.BasicAuth(self.api_key, self.api_secret)
            
            self.session = aiohttp.ClientSession(
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=30)
            )
            
            self.stats.rate_max = self.messages_per_second
            self.logger.info(f"✅ SMS API inicializada ({self.provider})")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inicializando SMS: {e}")
            return False
    
    async def cleanup(self):
        if self.session:
            await self.session.close()
    
    def _get_id_field(self) -> str:
        return 'telefono'
    
    def _render_message(self, template: str, datos: dict = None) -> str:
        """Renderiza mensaje con variables"""
        message = template or self.message_template
        if datos:
            for key, value in datos.items():
                message = message.replace(f"{{{key}}}", str(value))
        return message
    
    def _format_phone(self, numero: str) -> str:
        """Formatea número de teléfono"""
        numero = numero.strip()
        if not numero.startswith('+'):
            # Asumir código de país (ajustar según necesidad)
            numero = f"+{numero}"
        return numero
    
    async def _send_twilio(self, numero: str, message: str) -> dict:
        """Envía SMS via Twilio"""
        data = {
            "To": numero,
            "From": self.from_number,
            "Body": message
        }
        
        async with self.session.post(self.api_url, data=data) as response:
            result = await response.json()
            
            if response.status in (200, 201):
                return {
                    "success": True,
                    "message_id": result.get("sid"),
                    "status": result.get("status")
                }
            else:
                return {
                    "success": False,
                    "error": result.get("message", "Unknown error")
                }
    
    async def _send_vonage(self, numero: str, message: str) -> dict:
        """Envía SMS via Vonage (Nexmo)"""
        data = {
            "api_key": self.api_key,
            "api_secret": self.api_secret,
            "to": numero.replace("+", ""),
            "from": self.from_number,
            "text": message
        }
        
        async with self.session.post(self.api_url, json=data) as response:
            result = await response.json()
            
            messages = result.get("messages", [{}])
            if messages and messages[0].get("status") == "0":
                return {
                    "success": True,
                    "message_id": messages[0].get("message-id"),
                    "status": "sent"
                }
            else:
                return {
                    "success": False,
                    "error": messages[0].get("error-text", "Unknown error") if messages else "No response"
                }
    
    async def _send_generic(self, numero: str, message: str) -> dict:
        """Envía SMS via API genérica"""
        data = {
            "to": numero,
            "from": self.from_number,
            "message": message,
            "api_key": self.api_key
        }
        
        async with self.session.post(self.api_url, json=data) as response:
            result = await response.json()
            
            if response.status in (200, 201):
                return {
                    "success": True,
                    "message_id": result.get("id") or result.get("message_id"),
                    "status": result.get("status", "sent")
                }
            else:
                return {
                    "success": False,
                    "error": result.get("error") or result.get("message", "Unknown error")
                }
    
    async def send_single(self, item: dict) -> tuple:
        """Envía un SMS"""
        numero = item.get('telefono', '')
        datos = item.get('datos', {})
        
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
                    WHERE telefono = :numero AND estado NOT IN ('delivered')
                """), {"numero": numero})
        except Exception as e:
            self.release_active(numero)
            return (numero, False, {"error": str(e)})
        
        # Formatear y renderizar
        formatted_number = self._format_phone(numero)
        message = self._render_message(self.message_template, datos)
        
        # Enviar según proveedor
        try:
            if self.provider == "twilio":
                result = await self._send_twilio(formatted_number, message)
            elif self.provider == "vonage":
                result = await self._send_vonage(formatted_number, message)
            else:
                result = await self._send_generic(formatted_number, message)
            
            if result["success"]:
                with self.engine.begin() as conn:
                    conn.execute(text(f"""
                        UPDATE `{self.campaign_name}` 
                        SET estado = 'sent', message_id = :msg_id
                        WHERE telefono = :numero
                    """), {"msg_id": result.get("message_id", ""), "numero": numero})
                
                self.release_active(numero)
                return (numero, True, result)
            else:
                with self.engine.begin() as conn:
                    conn.execute(text(f"""
                        UPDATE `{self.campaign_name}` 
                        SET estado = 'failed', error = :error
                        WHERE telefono = :numero
                    """), {"error": result.get("error", "")[:200], "numero": numero})
                
                self.release_active(numero)
                return (numero, False, result)
                
        except Exception as e:
            self.release_active(numero)
            return (numero, False, {"error": str(e)})
    
    async def send_batch(self, items: list) -> list:
        """Envía lote de SMS"""
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
        """Procesa webhooks de status (Twilio, Vonage, etc)"""
        try:
            # Twilio format
            if "MessageSid" in webhook_data:
                message_id = webhook_data["MessageSid"]
                status = webhook_data.get("MessageStatus", "")
                
                estado = "sent"
                if status == "delivered":
                    estado = "delivered"
                elif status in ("failed", "undelivered"):
                    estado = "failed"
                
                with self.engine.begin() as conn:
                    conn.execute(text(f"""
                        UPDATE `{self.campaign_name}` 
                        SET estado = :estado, fecha_entrega = NOW()
                        WHERE message_id = :msg_id
                    """), {"estado": estado, "msg_id": message_id})
                    
        except Exception as e:
            self.logger.error(f"Error procesando webhook: {e}")
