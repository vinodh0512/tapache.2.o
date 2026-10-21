# Tepache Fermentation Backend (Render + MongoDB Atlas)

Production-ready, single-file Node.js ingestion backend for the ESP32 Tepache Fermentation Monitoring System.

## Features
- **MongoDB Atlas Integration**: Connects using Mongoose to your cluster.
- **Strict Idempotency**: Uses a compound unique index on `{ batch_id: 1, record_id: 1 }`. Retransmitted packets from the ESP32 will **never** generate duplicate database entries.
- **ESP32 Reachability**: `/health` endpoint for the 5-minute non-blocking connectivity verification.
- **Batch & Legacy Ingestion**: Supports both chunked batch payloads `{ batch_id, records: [...] }` and single-record payloads.
- **Live Glassmorphism Dashboard**: Embedded at `https://tapache.onrender.com/` with real-time charts (Chart.js), gauges, and recent logs.

---

## 1. Quick Local Setup

1. Open a terminal in the `backend/` directory:
   ```bash
   cd backend
   npm install
   ```

2. Start the server:
   ```bash
   npm start
   ```

3. Open your browser:
   - Dashboard: `http://localhost:5000/`
   - Health check: `http://localhost:5000/health`
   - Sensor data: `http://localhost:5000/api/sensor-data`

---

## 2. Deploying to Render (`https://tapache.onrender.com`)

1. Push your project or `backend` folder to GitHub.
2. In the [Render Dashboard](https://dashboard.render.com/):
   - Click **New** -> **Web Service**.
   - Connect your GitHub repository.
   - Set **Root Directory**: `backend` (if repo root is not `backend`).
   - Set **Build Command**: `npm install`.
   - Set **Start Command**: `node server.js`.
3. Add Environment Variables under **Environment**:
   - `MONGODB_URI`: `mongodb+srv://vinodht24cse_db_user:4UJdIy9KAA8fnptd@cluster0.eogvzbi.mongodb.net/tepache_db?retryWrites=true&w=majority&appName=Cluster0`
   - `PORT`: `10000` (Render defaults to this automatically)
4. Click **Deploy Web Service**.
