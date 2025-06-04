import re


class PhoneFormatter:
    """Klass för att hantera och validera telefonnummer"""

    @staticmethod
    def clean_phone_number(phone: str) -> str:
        """Tar bort specialtecken och ser till att numret endast innehåller siffror och +"""
        return re.sub(r"[^0-9+]", "", phone)

    @staticmethod
    def format_phone(phone: str, account_number: str) -> str:
        """Formaterar telefonnummer korrekt enligt regler"""
        phone = PhoneFormatter.clean_phone_number(phone)

        # Lägg endast till +46 om det saknas och kontot börjar med SE8902
        if account_number.startswith("SE8902") and not phone.startswith("+46"):
            phone = "+46" + phone.lstrip("0")

        return phone