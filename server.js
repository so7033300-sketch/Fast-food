// ==================================================================================
//  FAST-FOOD QR BUYURTMA TIZIMI — BACKEND SERVER
//  Node.js + Express + Socket.io + JSON fayl-baza (db.json)
//
//  Ushbu server quyidagilarni ta'minlaydi:
//   1) Mijoz uchun QR-menyu sahifasini va menyu ma'lumotlarini taqdim etadi.
//   2) Mijoz buyurtma bergach, kassa (admin) paneliga real-time (WebSocket) xabar yuboradi.
//   3) Kassir to'lovni tasdiqlagach, statistika (tushum/tannarx/foyda) avtomat yangilanadi.
//   4) Har oy boshlanganda joriy oy statistikasi avtomat arxivga o'tkaziladi va 0'dan boshlanadi.
// ==================================================================================

const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*', // Productionda faqat o'z domeningizni yozing, masalan: "https://mening-kafem.uz"
  },
});

// ----------------------------------------------------------------------------------
//  PORT SOZLAMASI
//  Hozircha lokal test uchun 3000-port ishlatiladi.
//  Hosting-provayder (Render, Railway, VPS va h.k.) o'ziga xos PORT beradi,
//  shuning uchun process.env.PORT orqali o'qish qoldirilgan — buni o'zgartirish shart emas.
// ----------------------------------------------------------------------------------
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// ----------------------------------------------------------------------------------
//  STATIK FAYLLAR (index.html, admin.html va boshqalar)
//  Bu loyihada "public" papkasi ISHLATILMAYDI — barcha fayllar server.js bilan
//  BITTA papkada turadi. Shuning uchun __dirname (joriy papka) to'g'ridan-to'g'ri
//  statik fayllar manzili sifatida ko'rsatilgan.
//
//  express.static socket.io.js kabi kutubxona fayllarini ham to'g'ri MIME-type
//  bilan uzatishi uchun bu qator kerak (masalan rasm, css fayllar bo'lsa).
// ----------------------------------------------------------------------------------
// MUHIM: { index: false } — bu bo'lmasa, express.static avtomat ravishda
// "/" manzili uchun index.html'ni to'g'ridan-to'g'ri ko'rsatib yuboradi va
// pastdagi app.get('/') orqali /menu?table=1'ga yo'naltirish ISHLAMAY QOLADI.
app.use(express.static(__dirname, { index: false }));

// ==================================================================================
//  MA'LUMOTLAR BAZASI (HOZIRGI HOLAT: JSON FAYL — db.json)
//
//  Bu loyihani darhol, hech qanday tashqi server/baza o'rnatmasdan ishga tushirish
//  imkonini beradi. Barcha buyurtmalar va statistika db.json fayliga yoziladi.
//
//  ==============================================================================
//  KELAJAKDA MONGODB'GA O'TISH UCHUN:
//  1) Terminalda:  npm install mongoose
//  2) Quyidagi qatorlarni faollashtiring va o'zingizning ulanish manzilingizni kiriting:
//
//     const mongoose = require('mongoose');
//     mongoose.connect("SHU_JOYGA_O'Z_MONGODB_URI_LINKINGIZNI_YOZING", {
//       useNewUrlParser: true,
//       useUnifiedTopology: true,
//     }).then(() => console.log('MongoDB ulandi'))
//       .catch(err => console.error('MongoDB xatosi:', err));
//
//  3) Keyin readDB()/writeDB() funksiyalari o'rniga Mongoose model
//     (Order.find(), Order.create(), va h.k.) chaqiruvlariga almashtiring.
//
//  // SHU JOYGA O'Z LINKINGIZNI YOZING (MongoDB Atlas connection string)
//  ==============================================================================
// ==================================================================================

const DB_PATH = path.join(__dirname, 'db.json');

function getDefaultDB() {
  const now = new Date();
  return {
    orders: [],
    stats: {
      currentYear: now.getFullYear(),
      currentMonth: now.getMonth() + 1,
      totalRevenue: 0,
      totalCost: 0,
      totalProfit: 0,
    },
    archive: [],
  };
}

function readDB() {
  if (!fs.existsSync(DB_PATH)) {
    const initial = getDefaultDB();
    fs.writeFileSync(DB_PATH, JSON.stringify(initial, null, 2));
    return initial;
  }
  const raw = fs.readFileSync(DB_PATH, 'utf-8');
  try {
    return JSON.parse(raw);
  } catch (e) {
    console.error('db.json faylini o\'qishda xato, yangi baza yaratilmoqda:', e);
    const initial = getDefaultDB();
    fs.writeFileSync(DB_PATH, JSON.stringify(initial, null, 2));
    return initial;
  }
}

function writeDB(db) {
  fs.writeFileSync(DB_PATH, JSON.stringify(db, null, 2));
}

// ----------------------------------------------------------------------------------
//  OYLIK HISOBOTNI AVTOMAT TEKSHIRISH VA ARXIVLASH FUNKSIYASI
//
//  Har safar (a) buyurtma to'langanda, (b) statistika so'ralganda va
//  (c) har soatda avtomat (setInterval orqali) ushbu funksiya chaqiriladi.
//  Agar joriy sana (yil/oy) bazadagi "joriy oy"dan farq qilsa:
//    - Eski oyning yakuniy hisobotlari (Yil, Oy, Tushum, Tannarx, Foyda) ARXIVGA tushadi.
//    - Joriy oy hisoblagichlari (tushum, tannarx, foyda) 0 ga tushiriladi.
// ----------------------------------------------------------------------------------
function checkMonthRollover(db) {
  const now = new Date();
  const realYear = now.getFullYear();
  const realMonth = now.getMonth() + 1;

  const monthChanged =
    db.stats.currentYear !== realYear || db.stats.currentMonth !== realMonth;

  if (monthChanged) {
    const hadActivity = db.stats.totalRevenue > 0 || db.stats.totalCost > 0;

    if (hadActivity) {
      db.archive.unshift({
        year: db.stats.currentYear,
        month: db.stats.currentMonth,
        totalRevenue: db.stats.totalRevenue,
        totalCost: db.stats.totalCost,
        totalProfit: db.stats.totalProfit,
        archivedAt: now.toISOString(),
      });
    }

    db.stats.currentYear = realYear;
    db.stats.currentMonth = realMonth;
    db.stats.totalRevenue = 0;
    db.stats.totalCost = 0;
    db.stats.totalProfit = 0;

    writeDB(db);
    console.log(`[OYLIK ARXIV] Yangi oy aniqlandi: ${realMonth}/${realYear}. Statistika 0 ga tushirildi.`);
  }

  return db;
}

// ==================================================================================
//  FAST-FOOD MENYUSI
//  "cost" (tannarx) maydoni faqat sof foyda hisobini yuritish uchun ishlatiladi va
//  mijozga ko'rsatiladigan /api/menu javobida YASHIRIB qo'yiladi (xavfsizlik uchun).
//  Narxlarni va taomlarni bemalol o'zgartiring/ko'paytiring.
// ==================================================================================
const MENU = [
  // ---------------- BURGERLAR ----------------
  { id: 'burger-classic', category: 'Burgerlar', name: 'Classic Burger', price: 25000, cost: 12000, emoji: '🍔', description: 'Mol go\'shti, pomidor, salat, maxsus sous' },
  { id: 'burger-cheese', category: 'Burgerlar', name: 'Cheeseburger', price: 28000, cost: 14000, emoji: '🍔', description: 'Mol go\'shti, erigan pishloq, tuzlangan bodring' },
  { id: 'burger-double', category: 'Burgerlar', name: 'Double Burger', price: 35000, cost: 18000, emoji: '🍔', description: 'Ikki qavat go\'sht, ikki qavat pishloq' },
  { id: 'burger-chicken', category: 'Burgerlar', name: 'Chicken Burger', price: 24000, cost: 11000, emoji: '🍗', description: 'Qarsildoq tovuq file, salat, sous' },

  // ---------------- LAVASH ----------------
  { id: 'lavash-chicken', category: 'Lavash', name: 'Tovuqli Lavash', price: 22000, cost: 10000, emoji: '🌯', description: 'Tovuq go\'shti, sabzavotlar, sous' },
  { id: 'lavash-beef', category: 'Lavash', name: "Go'shtli Lavash", price: 26000, cost: 13000, emoji: '🌯', description: "Mol go'shti, sabzavotlar, achchiq sous" },
  { id: 'lavash-mix', category: 'Lavash', name: 'Mix Lavash', price: 28000, cost: 14000, emoji: '🌯', description: "Tovuq va mol go'shti aralashmasi" },

  // ---------------- QO'SHIMCHA TAOMLAR ----------------
  { id: 'fries-small', category: "Qo'shimcha", name: 'Kartoshka Fri (kichik)', price: 12000, cost: 5000, emoji: '🍟', description: 'Qarsildoq kartoshka fri' },
  { id: 'fries-big', category: "Qo'shimcha", name: 'Kartoshka Fri (katta)', price: 18000, cost: 7500, emoji: '🍟', description: 'Qarsildoq kartoshka fri, katta portsiya' },
  { id: 'nuggets', category: "Qo'shimcha", name: 'Chicken Nuggets (6 dona)', price: 20000, cost: 9000, emoji: '🍗', description: 'Qarsildoq tovuq nagetslari' },

  // ---------------- ICHIMLIKLAR ----------------
  { id: 'cola', category: 'Ichimliklar', name: 'Coca-Cola 0.5L', price: 8000, cost: 3000, emoji: '🥤', description: 'Sovutilgan gazli ichimlik' },
  { id: 'fanta', category: 'Ichimliklar', name: 'Fanta 0.5L', price: 8000, cost: 3000, emoji: '🥤', description: 'Sovutilgan gazli ichimlik' },
  { id: 'sprite', category: 'Ichimliklar', name: 'Sprite 0.5L', price: 8000, cost: 3000, emoji: '🥤', description: 'Sovutilgan gazli ichimlik' },
  { id: 'water', category: 'Ichimliklar', name: 'Ichimlik suvi 0.5L', price: 4000, cost: 1500, emoji: '💧', description: 'Toza ichimlik suvi' },
  { id: 'ayran', category: 'Ichimliklar', name: 'Ayron 0.5L', price: 6000, cost: 2500, emoji: '🥛', description: "An'anaviy ayron" },
];

function findMenuItem(id) {
  return MENU.find((item) => item.id === id);
}

// ==================================================================================
//  YORDAMCHI FUNKSIYA — buyurtma ID generatsiyasi
// ==================================================================================
function generateOrderId() {
  return 'ORD-' + Date.now() + '-' + Math.floor(Math.random() * 1000);
}

// ==================================================================================
//  ROUTE'LAR
// ==================================================================================

// Mijoz QR-menyu sahifasi: /menu?table=4
// index.html server.js bilan BITTA papkada turgani uchun __dirname'dan to'g'ridan-to'g'ri o'qiladi.
app.get('/menu', (req, res) => {
  res.sendFile(__dirname + '/index.html');
});

// Kassir (admin) paneli
// admin.html server.js bilan BITTA papkada turgani uchun __dirname'dan to'g'ridan-to'g'ri o'qiladi.
app.get('/admin', (req, res) => {
  res.sendFile(__dirname + '/admin.html');
});

// Bosh sahifa — to'g'ridan-to'g'ri menyuga yo'naltiramiz
app.get('/', (req, res) => {
  res.redirect('/menu?table=1');
});

// --------------------------- API: MENYUNI OLISH ---------------------------
// Mijoz tomoniga "cost" (tannarx) maydoni YUBORILMAYDI — bu ichki ma'lumot.
app.get('/api/menu', (req, res) => {
  const publicMenu = MENU.map(({ id, category, name, price, emoji, description }) => ({
    id,
    category,
    name,
    price,
    emoji,
    description,
  }));
  res.json(publicMenu);
});

// --------------------------- API: BUYURTMA YARATISH ---------------------------
// Mijoz savatini yuboradi: { table: 4, items: [{ id: 'burger-classic', qty: 2 }, ...] }
app.post('/api/orders', (req, res) => {
  const { table, items } = req.body;

  if (!table) {
    return res.status(400).json({ error: 'Stol raqami ko\'rsatilmagan (?table=... URL manzilida bo\'lishi kerak).' });
  }
  if (!Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: 'Savat bo\'sh. Buyurtma berish uchun kamida bitta taom tanlang.' });
  }

  // Narxlarni SERVER TOMONDA hisoblaymiz — mijoz brauzerdan noto'g'ri narx yuborishining oldini olish uchun.
  const orderItems = [];
  let total = 0;
  let totalCost = 0;

  for (const reqItem of items) {
    const menuItem = findMenuItem(reqItem.id);
    if (!menuItem) continue; // Noma'lum taom ID'si e'tiborsiz qoldiriladi
    const qty = Math.max(1, parseInt(reqItem.qty, 10) || 1);

    orderItems.push({
      id: menuItem.id,
      name: menuItem.name,
      price: menuItem.price,
      qty,
      lineTotal: menuItem.price * qty,
    });

    total += menuItem.price * qty;
    totalCost += menuItem.cost * qty;
  }

  if (orderItems.length === 0) {
    return res.status(400).json({ error: 'Savatdagi taomlar menyuda topilmadi.' });
  }

  const order = {
    id: generateOrderId(),
    table: String(table),
    items: orderItems,
    total,
    totalCost,
    // -----------------------------------------------------------------
    // TO'LOV USULI: Hozircha faqat "Naqd pul / Terminal" (kassada to'lash).
    //
    // Kelajakda Click yoki Payme kabi onlayn to'lov tizimlarini ulash uchun:
    //  - Mijoz "To'lash" bosganda shu yerga onlayn to'lov so'rovi yuboriladi,
    //  - To'lov muvaffaqiyatli bo'lgach, webhook orqali status='paid' qilinadi.
    //
    // // SHU JOYGA O'Z LINKINGIZNI YOZING (Click/Payme API endpoint manzili)
    // -----------------------------------------------------------------
    paymentMethod: 'naqd_terminal',
    status: 'new', // 'new' -> 'paid'
    createdAt: new Date().toISOString(),
    paidAt: null,
  };

  const db = readDB();
  db.orders.push(order);
  writeDB(db);

  // Kassa (admin) paneliga LAHZADA xabar
  io.to('admin-room').emit('new-order', order);

  // Mijozning o'ziga ham tasdiqlovchi signal (agar u socket orqali ulangan bo'lsa)
  io.to('table-' + order.table).emit('order-created', order);

  res.status(201).json(order);
});

// --------------------------- API: BARCHA BUYURTMALARNI OLISH (admin uchun) ---------------------------
app.get('/api/orders', (req, res) => {
  const db = readDB();
  const sorted = [...db.orders].sort(
    (a, b) => new Date(b.createdAt) - new Date(a.createdAt)
  );
  res.json(sorted);
});

// --------------------------- API: TO'LOVNI TASDIQLASH ---------------------------
app.post('/api/orders/:id/pay', (req, res) => {
  const { id } = req.params;
  let db = readDB();

  const order = db.orders.find((o) => o.id === id);
  if (!order) {
    return res.status(404).json({ error: 'Buyurtma topilmadi.' });
  }
  if (order.status === 'paid') {
    return res.status(400).json({ error: 'Bu buyurtma allaqachon to\'langan.' });
  }

  // To'lovni yozishdan oldin, oy almashganligini tekshirib olamiz
  db = checkMonthRollover(db);

  order.status = 'paid';
  order.paidAt = new Date().toISOString();

  db.stats.totalRevenue += order.total;
  db.stats.totalCost += order.totalCost;
  db.stats.totalProfit = db.stats.totalRevenue - db.stats.totalCost;

  writeDB(db);

  // Admin panelidagi barcha ochiq oynalarga statistikani yangilash uchun signal
  io.to('admin-room').emit('order-paid', order);
  io.to('admin-room').emit('stats-update', { stats: db.stats, archive: db.archive });

  // Mijoz ekraniga ham "to'landi" signali (agar kerak bo'lsa)
  io.to('table-' + order.table).emit('order-paid', order);

  res.json(order);
});

// --------------------------- API: BUYURTMANI BEKOR QILISH (ixtiyoriy, admin uchun) ---------------------------
app.post('/api/orders/:id/cancel', (req, res) => {
  const { id } = req.params;
  const db = readDB();
  const order = db.orders.find((o) => o.id === id);
  if (!order) {
    return res.status(404).json({ error: 'Buyurtma topilmadi.' });
  }
  if (order.status === 'paid') {
    return res.status(400).json({ error: 'To\'langan buyurtmani bekor qilib bo\'lmaydi.' });
  }
  order.status = 'cancelled';
  writeDB(db);
  io.to('admin-room').emit('order-cancelled', order);
  io.to('table-' + order.table).emit('order-cancelled', order);
  res.json(order);
});

// --------------------------- API: STATISTIKA VA ARXIVNI OLISH ---------------------------
app.get('/api/stats', (req, res) => {
  let db = readDB();
  db = checkMonthRollover(db); // Sahifa ochilganda ham tekshirib qo'yamiz
  res.json({ stats: db.stats, archive: db.archive });
});

// ==================================================================================
//  SOCKET.IO — REAL-TIME ALOQA
// ==================================================================================
io.on('connection', (socket) => {
  console.log('Yangi ulanish:', socket.id);

  // Kassir (admin) paneli o'zini "admin-room" xonasiga qo'shadi
  socket.on('join-admin', () => {
    socket.join('admin-room');
  });

  // Mijoz o'z stoli xonasiga qo'shiladi (masalan "table-4")
  socket.on('join-table', (table) => {
    socket.join('table-' + table);
  });

  socket.on('disconnect', () => {
    console.log('Ulanish uzildi:', socket.id);
  });
});

// ----------------------------------------------------------------------------------
//  HAR SOATDA AVTOMAT OYLIK ARXIV TEKSHIRUVI
//  Bu admin panel yopiq bo'lsa ham, tungi soat 00:00 da oy almashganda
//  statistika to'g'ri arxivlanishini ta'minlaydi.
// ----------------------------------------------------------------------------------
setInterval(() => {
  let db = readDB();
  const before = JSON.stringify(db.stats);
  db = checkMonthRollover(db);
  const after = JSON.stringify(db.stats);
  if (before !== after) {
    io.to('admin-room').emit('stats-update', { stats: db.stats, archive: db.archive });
  }
}, 60 * 60 * 1000); // Har 1 soatda

server.listen(PORT, () => {
  console.log('==============================================');
  console.log(`  FAST-FOOD QR BUYURTMA TIZIMI ISHGA TUSHDI`);
  console.log(`  Mijoz menyusi:  http://localhost:${PORT}/menu?table=4`);
  console.log(`  Kassa paneli:   http://localhost:${PORT}/admin`);
  console.log('==============================================');
});
