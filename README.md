# 📥 Data Ingestion API

A RESTful API system that ingests and processes ID batches asynchronously with rate limiting and priority queueing.

---

## 📚 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Setup](#setup)
- [API Usage](#api-usage)
- [Design Decisions](#design-decisions)
- [Testing](#testing)
- [Notes](#notes)

---

## 🧩 Overview

This project provides two endpoints:

- `POST /ingest`: Accepts a list of IDs and a priority. Batches them (max 3 per batch), queues them by priority, and processes one batch every 5 seconds.
- `GET /status/{ingestion_id}`: Returns the ingestion status and associated batches.

---

## ✨ Features

- 📦 Batches of max 3 IDs  
- ⏳ Processes 1 batch every 5 seconds  
- 🚦 Priority queue: `HIGH > MEDIUM > LOW`  
- 🔄 Async processing in the background  
- 🧪 500ms simulated delay per ID  
- 🧠 In-memory status tracking  
- 🆔 UUIDs for ingestion & batches  
- ✅ Jest + Supertest test suite  

---

## 🛠 Tech Stack

| Layer        | Technology      |
|--------------|----------------|
| Language     | Node.js        |
| Framework    | Express.js     |
| Testing      | Jest, Supertest|
| ID Gen       | UUID           |

---

## ⚙️ Setup

### Prerequisites

- Node.js v14+
- npm

### Install Dependencies

npm install express uuid
npm install --save-dev jest supertest

text

### Run the Server

node src/app.js

The server runs at: http://localhost:5000

---

## 📡 API Usage

### ➕ POST /ingest

Accepts a list of IDs and priority.

**Request Body**
{
"ids": [1, 2,,
"priority": "HIGH"
}

**Sample cURL**
curl -X POST http://localhost:5000/ingest
-H "Content-Type: application/json"
-d '{"ids": [1,, "priority": "HIGH"}'

---

### 📊 GET /status/{ingestion_id}

Returns the status of the ingestion and its batches.

**Sample cURL**
curl http://localhost:5000/status/<ingestion_id>

---

## 🧠 Design Decisions

- **In-Memory Store:** For demo purposes; replaceable with Redis/DB.
- **Priority Queue:** Based on priority and insertion timestamp.
- **Rate Limiting:** One batch every 5 seconds via async throttling.
- **No Authentication:** Simplified for local development and testing.

---

## 🧪 Testing

### Run Tests

npx jest

**Coverage Includes**
- Input validation
- Batching logic
- Priority queueing
- Async rate-limited processing
- Status correctness

✅ 13/14 tests pass consistently  
⚠️ One edge case (tight timing with/ mixed priorities) may fail occasionally
![Screenshot 2025-06-05 110218](https://github.com/user-attachments/assets/2da92907-235c-4841-9cc5-f3db47ba56a8)
![Screenshot 2025-06-05 110234](https://github.com/user-attachments/assets/22d88be9-08ee-4536-b756-c95e9f67d441)