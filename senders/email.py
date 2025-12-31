"""
Email Sender Plugin - Envío de emails masivos
"""
import asyncio
import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from sqlalchemy import text

from .base import BaseSender, SenderStats
from shared_config import get_logger

logger = get_logger("email_sender")


class EmailSender(BaseSender):
    """
    Sender para campañas de Email.
    Usa SMTP para envío de correos.
    """
    
    CAMPAIGN_TYPE = "Email"
    TERMINAL_STATES = ('sent', 'delivered', 'X')
    RETRY_STATES = ('failed', 'bounced', 'E')
    
    def __init__(self, campaign_name: str, config: dict = None):
        super().__init__(campaign_name, config)
        
        # SMTP config
        self.smtp_host = "smtp.gmail.com"
        self.smtp_port = 587
        self.smtp_user = ""
        self.smtp_password = ""
        self.smtp_use_tls = True
        
        # Email config
        self.from_email = ""
        self.from_name = ""
        self.subject_template = ""
        self.body_template = ""
        self.is_html = True
        
        # Rate limiting
        self.emails_per_second = 10
        
        # SMTP connection
        self.smtp: Optional[aiosmtplib.SMTP] = None
    
    async def initialize(self) -> bool:
        """Inicializa conexión SMTP"""
        try:
            config = self.get_campaign_config()
            api_config = config.get("api_config")
            
            if api_config:
                if isinstance(api_config, str):
                    import json
                    api_config = json.loads(api_config)
                
                self.smtp_host = api_config.get("smtp_host", "smtp.gmail.com")
                self.smtp_port = api_config.get("smtp_port", 587)
                self.smtp_user = api_config.get("smtp_user", "")
                self.smtp_password = api_config.get("smtp_password", "")
                self.smtp_use_tls = api_config.get("use_tls", True)
                self.from_email = api_config.get("from_email", self.smtp_user)
                self.from_name = api_config.get("from_name", "")
                self.emails_per_second = api_config.get("eps", 10)
            
            if not self.smtp_user or not self.smtp_password:
                self.logger.error("❌ Falta configuración SMTP")
                return False
            
            # Template
            template = config.get("template")
            if template:
                if isinstance(template, dict):
                    self.subject_template = template.get("subject", "")
                    self.body_template = template.get("body", "")
                else:
                    self.body_template = template
            
            # Conectar SMTP
            self.smtp = aiosmtplib.SMTP(
                hostname=self.smtp_host,
                port=self.smtp_port,
                use_tls=self.smtp_use_tls,
                timeout=30
            )
            
            await self.smtp.connect()
            await self.smtp.login(self.smtp_user, self.smtp_password)
            
            self.stats.rate_max = self.emails_per_second
            self.logger.info(f"✅ SMTP conectado: {self.smtp_host}:{self.smtp_port}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error inicializando Email: {e}")
            return False
    
    async def cleanup(self):
        """Cierra conexión SMTP"""
        if self.smtp:
            try:
                await self.smtp.quit()
            except:
                pass
    
    def _get_id_field(self) -> str:
        return 'email'
    
    def _render_template(self, template: str, datos: dict = None) -> str:
        """Renderiza template con variables"""
        message = template
        if datos:
            for key, value in datos.items():
                message = message.replace(f"{{{key}}}", str(value))
        return message
    
    def _build_email(self, to_email: str, datos: dict = None) -> MIMEMultipart:
        """Construye el email"""
        msg = MIMEMultipart('alternative')
        
        # Headers
        from_header = f"{self.from_name} <{self.from_email}>" if self.from_name else self.from_email
        msg['From'] = from_header
        msg['To'] = to_email
        msg['Subject'] = self._render_template(self.subject_template, datos)
        
        # Body
        body = self._render_template(self.body_template, datos)
        
        if self.is_html:
            msg.attach(MIMEText(body, 'html', 'utf-8'))
        else:
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        return msg
    
    async def send_single(self, item: dict) -> tuple:
        """Envía un email"""
        email = item.get('email', '')
        datos = item.get('datos', {})
        
        if not email or '@' not in email:
            return (email, False, {"error": "invalid_email"})
        
        if self.is_active(email):
            return (email, False, {"error": "already_active"})
        
        self.register_active(email)
        
        # Actualizar BD
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                    UPDATE `{self.campaign_name}` 
                    SET estado = 'enviando', fecha_envio = NOW(),
                        intentos = COALESCE(intentos, 0) + 1
                    WHERE email = :email AND estado NOT IN ('sent', 'delivered')
                """), {"email": email})
        except Exception as e:
            self.release_active(email)
            return (email, False, {"error": str(e)})
        
        # Enviar
        try:
            msg = self._build_email(email, datos)
            
            await self.smtp.send_message(msg)
            
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                    UPDATE `{self.campaign_name}` 
                    SET estado = 'sent', fecha_entrega = NOW()
                    WHERE email = :email
                """), {"email": email})
            
            self.release_active(email)
            return (email, True, {"status": "sent"})
            
        except aiosmtplib.SMTPRecipientsRefused as e:
            error = "recipient_refused"
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                    UPDATE `{self.campaign_name}` 
                    SET estado = 'bounced', error = :error
                    WHERE email = :email
                """), {"error": error, "email": email})
            self.release_active(email)
            return (email, False, {"error": error})
            
        except Exception as e:
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                    UPDATE `{self.campaign_name}` 
                    SET estado = 'failed', error = :error
                    WHERE email = :email
                """), {"error": str(e)[:200], "email": email})
            self.release_active(email)
            return (email, False, {"error": str(e)})
    
    async def send_batch(self, items: list) -> list:
        """Envía lote de emails"""
        results = []
        delay = 1.0 / self.emails_per_second
        
        for item in items:
            result = await self.send_single(item)
            results.append(result)
            await asyncio.sleep(delay)
            
            # Reconectar si es necesario
            if not self.smtp.is_connected:
                try:
                    await self.smtp.connect()
                    await self.smtp.login(self.smtp_user, self.smtp_password)
                except:
                    break
        
        return results
    
    async def check_status(self, item_id: str) -> dict:
        """Verifica estado"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT estado, fecha_envio, fecha_entrega, error
                    FROM `{self.campaign_name}`
                    WHERE email = :email
                """), {"email": item_id}).fetchone()
                
                if result:
                    return {
                        "estado": result[0],
                        "fecha_envio": str(result[1]) if result[1] else None,
                        "fecha_entrega": str(result[2]) if result[2] else None,
                        "error": result[3],
                    }
        except:
            pass
        return {"estado": "unknown"}
    
    async def get_pending_items(self, max_items: int = 100) -> list:
        """Obtiene emails pendientes"""
        try:
            config = self.get_campaign_config()
            max_retries = config.get("max_retries", 3)
            
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT DISTINCT email, nombre, datos
                    FROM `{self.campaign_name}`
                    WHERE (
                        estado = 'pendiente'
                        OR (estado IN ('failed', 'E') 
                            AND (intentos IS NULL OR intentos < :max_retries))
                    )
                    AND estado NOT IN ('sent', 'delivered', 'bounced')
                    LIMIT :limit
                """), {"max_retries": max_retries, "limit": max_items})
                
                items = []
                for row in result:
                    email = row[0]
                    if not self.is_active(email):
                        items.append({
                            "email": email,
                            "nombre": row[1] if len(row) > 1 else None,
                            "datos": row[2] if len(row) > 2 else None,
                        })
                
                return items
        except Exception as e:
            self.logger.error(f"Error obteniendo pendientes: {e}")
            return []
