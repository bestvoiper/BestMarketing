"""
Senders Package - Plugins para diferentes tipos de campañas
"""
from .base import BaseSender, SenderStats
from .audio import AudioSender
from .dialer import DialerSender
from .whatsapp import WhatsAppSender
from .telegram import TelegramSender
from .facebook import FacebookSender
from .email import EmailSender
from .sms import SMSSender

# Registro de senders por tipo de campaña
SENDER_REGISTRY = {
    'Audio': AudioSender,
    'Discador': DialerSender,
    'WhatsApp': WhatsAppSender,
    'Telegram': TelegramSender,
    'Facebook': FacebookSender,
    'Email': EmailSender,
    'SMS': SMSSender,
}

def get_sender(campaign_type: str) -> type:
    """Obtiene la clase de sender para un tipo de campaña"""
    return SENDER_REGISTRY.get(campaign_type)

def get_available_types() -> list:
    """Lista de tipos de campaña disponibles"""
    return list(SENDER_REGISTRY.keys())

__all__ = [
    'BaseSender',
    'SenderStats',
    'AudioSender',
    'DialerSender',
    'WhatsAppSender',
    'TelegramSender',
    'FacebookSender',
    'EmailSender',
    'SMSSender',
    'SENDER_REGISTRY',
    'get_sender',
    'get_available_types',
]
