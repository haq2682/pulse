For Development (Self-Signed)
# Create nginx directory structure
mkdir -p nginx/ssl

# Generate self-signed certificate
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout nginx/ssl/key.pem \
  -out nginx/ssl/cert.pem \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"

# Set permissions
chmod 644 nginx/ssl/cert.pem
chmod 600 nginx/ssl/key.pem


For Production (Let's Encrypt)
# Install certbot (on host machine)
sudo apt-get install certbot

# Get certificate (domain must point to your server)
sudo certbot certonly --standalone -d nifi.yourdomain.com

# Copy to nginx directory
sudo cp /etc/letsencrypt/live/nifi.yourdomain.com/fullchain.pem nginx/ssl/cert.pem
sudo cp /etc/letsencrypt/live/nifi.yourdomain.com/privkey.pem nginx/ssl/key.pem

# Set permissions
sudo chmod 644 nginx/ssl/cert.pem
sudo chmod 600 nginx/ssl/key.pem
sudo chown $USER:$USER nginx/ssl/*.pem









# 1. Ensure .env is set for development
cat .env | grep NGINX
# Should show:
# NGINX_MODE=development
# NGINX_DOMAIN=localhost
# NGINX_HTTP_PORT=8082

# 2. Create SSL directory (even for dev, to prevent errors)
mkdir -p nginx/ssl
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout nginx/ssl/key.pem \
  -out nginx/ssl/cert.pem \
  -subj "/C=US/ST=State/L=City/O=Org/CN=localhost"

# 3. Start all services
docker-compose up -d

# 4. Verify Nginx is running
docker logs nginx-proxy

# 5. Test health check
curl http://localhost:8082/health
# Should return: OK

# 6. Test upload from browser
# Navigate to http://localhost:5173
# Upload a file and check browser console for success










# 1. Update .env for production
nano .env
# Set:
# NGINX_MODE=production
# NGINX_DOMAIN=nifi.yourdomain.com
# NGINX_HTTP_PORT=80
# NGINX_HTTPS_PORT=443

# 2. Get SSL certificates
sudo certbot certonly --standalone -d nifi.yourdomain.com

# Copy certificates
sudo cp /etc/letsencrypt/live/nifi.yourdomain.com/fullchain.pem nginx/ssl/cert.pem
sudo cp /etc/letsencrypt/live/nifi.yourdomain.com/privkey.pem nginx/ssl/key.pem
sudo chown $USER:$USER nginx/ssl/*.pem
chmod 644 nginx/ssl/cert.pem
chmod 600 nginx/ssl/key.pem

# 3. Update frontend .env.production
echo "VITE_NIFI_UPLOAD_URL=https://nifi.yourdomain.com/upload" > frontend/.env.production

# 4. Rebuild frontend with production config
docker-compose build frontend

# 5. Start all services
docker-compose up -d

# 6. Verify HTTPS is working
curl https://nifi.yourdomain.com/health
# Should return: OK

# 7. Test upload from browser
# Navigate to your production URL and test upload