import aiosmtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import get_settings

settings = get_settings()

class EmailService:
    @staticmethod
    async def send_password_reset_email(to_email: str, reset_token:  str, user_name: str):
        """Send password reset email."""
        reset_link = f"{settings.frontend_url}/reset-password? token={reset_token}"
        
        message = MIMEMultipart("alternative")
        message["Subject"] = "Reset Your Password - Pulse Analytics"
        message["From"] = settings.from_email
        message["To"] = to_email
        
        html_content = f"""
        <! DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                . container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                           color: white; padding: 30px; text-align: center; border-radius: 10px 10px 0 0; }}
                .content {{ background: #f9f9f9; padding: 30px; border-radius: 0 0 10px 10px; }}
                .button {{ display: inline-block; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                           color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px;
                           margin: 20px 0; }}
                .footer {{ text-align: center; color: #888; font-size: 12px; margin-top: 20px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Pulse Analytics</h1>
                </div>
                <div class="content">
                    <h2>Hello {user_name},</h2>
                    <p>We received a request to reset your password. Click the button below to create a new password:</p>
                    <p style="text-align: center;">
                        <a href="{reset_link}" class="button">Reset Password</a>
                    </p>
                    <p>This link will expire in {settings.password_reset_expire_minutes} minutes.</p>
                    <p>If you didn't request this, you can safely ignore this email.</p>
                </div>
                <div class="footer">
                    <p>© 2025 Pulse Analytics. All rights reserved.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        message. attach(MIMEText(html_content, "html"))
        
        await aiosmtplib.send(
            message,
            hostname=settings.smtp_host,
            port=settings.smtp_port,
            username=settings.smtp_user,
            password=settings.smtp_password,
            start_tls=True
        )

email_service = EmailService()