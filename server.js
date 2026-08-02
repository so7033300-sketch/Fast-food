// ==================================================================================
//  FAST-FOOD QR BUYURTMA TIZIMI — BACKEND SERVER
//  Node.js + Express + Socket.io + JSON fayl-baza
//
//  Ushbu server quyidagilarni ta'minlaydi:
//   1) Mijoz uchun QR-menyu sahifasini va menyu ma'lumotlarini taqdim etadi.
//   2) Mijoz buyurtma bergach, kassa (admin) paneliga real-time (WebSocket) xabar yuboradi.
//   3) Kassir to'lovni tasdiqlagach, statistika (tushum/tannarx/foyda) avtomat yangilanadi.
//   4) Har oy boshlanganda joriy oy statistikasi avtomat arxivga o'tkaziladi va 0'dan boshlanadi.
//   5) Ombordan (inventar) avtomat ayirish va kam qolganda ogohlantirish.
//
//  MA'LUMOTLAR DOIMIY SAQLANISHI HAQIDA (MUHIM):
//  Render'ning bepul tarifida fayl tizimi vaqtinchalik (ephemeral) — server qayta
//  ishga tushganda barcha fayllar o'chib ketadi. Buning oldini olish uchun Render'da
//  pullik tarifga o'tib, "Persistent Disk" ulang:
//    1) Render Dashboard -> xizmatingiz -> "Disks" -> "Add Disk"
//    2) Mount Path: masalan /var/data (o'zingiz xohlagan nom)
//    3) Keyin "Environment" bo'limiga DATA_DIR nomli o'zgaruvchi qo'shing,
//       qiymati xuddi shu mount path bilan bir xil bo'lsin: /var/data
//  Shundan keyin barcha ma'lumotlar (db.json, menu.json, ombor.json) doimiy
//  diskka yoziladi va server qayta ishga tushsa ham yo'qolmaydi.
//  Agar DATA_DIR sozlanmagan bo'lsa, fayllar oddiy holatda joriy papkaga yoziladi
//  (lokal test uchun to'liq yetarli, lekin bepul Render'da vaqtinchalik bo'ladi).
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
// ----------------------------------------------------------------------------------
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '15mb' })); // Taom rasmlari (base64) uchun limit oshirilgan

// ----------------------------------------------------------------------------------
//  STATIK FAYLLAR (index.html, admin.html va boshqalar)
// ----------------------------------------------------------------------------------
app.use(express.static(__dirname, { index: false }));

// ==================================================================================
//  MA'LUMOTLAR SAQLANADIGAN JOY
//  DATA_DIR muhit o'zgaruvchisi sozlangan bo'lsa (Render Persistent Disk mount path),
//  fayllar o'sha yerga yoziladi. Sozlanmagan bo'lsa, joriy papka ishlatiladi.
// ==================================================================================
const DATA_DIR = process.env.DATA_DIR || __dirname;

// Agar DATA_DIR mavjud bo'lmasa (masalan birinchi marta), papkani yaratib qo'yamiz
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

const DB_PATH = path.join(DATA_DIR, 'db.json');
const MENU_PATH = path.join(DATA_DIR, 'menu.json');
const OMBOR_PATH = path.join(DATA_DIR, 'ombor.json');

function getDefaultStats() {
  const now = new Date();
  return {
    currentYear: now.getFullYear(),
    currentMonth: now.getMonth() + 1,
    totalRevenue: 0,
    totalCost: 0,
    totalProfit: 0,
  };
}

function readDB() {
  if (!fs.existsSync(DB_PATH)) {
    const initial = { orders: [], stats: getDefaultStats(), archive: [] };
    fs.writeFileSync(DB_PATH, JSON.stringify(initial, null, 2));
    return initial;
  }
  try {
    return JSON.parse(fs.readFileSync(DB_PATH, 'utf-8'));
  } catch (e) {
    console.error('db.json faylini o\'qishda xato, yangi baza yaratilmoqda:', e);
    const initial = { orders: [], stats: getDefaultStats(), archive: [] };
    fs.writeFileSync(DB_PATH, JSON.stringify(initial, null, 2));
    return initial;
  }
}
function writeDB(db) {
  fs.writeFileSync(DB_PATH, JSON.stringify(db, null, 2));
}

function readMenu() {
  if (!fs.existsSync(MENU_PATH)) {
    fs.writeFileSync(MENU_PATH, JSON.stringify([], null, 2));
    return [];
  }
  try {
    return JSON.parse(fs.readFileSync(MENU_PATH, 'utf-8'));
  } catch (e) {
    console.error('menu.json faylini o\'qishda xato:', e);
    return [];
  }
}
function writeMenu(menu) {
  fs.writeFileSync(MENU_PATH, JSON.stringify(menu, null, 2));
}

function readOmbor() {
  if (!fs.existsSync(OMBOR_PATH)) {
    fs.writeFileSync(OMBOR_PATH, JSON.stringify([], null, 2));
    return [];
  }
  try {
    return JSON.parse(fs.readFileSync(OMBOR_PATH, 'utf-8'));
  } catch (e) {
    console.error('ombor.json faylini o\'qishda xato:', e);
    return [];
  }
}
function writeOmbor(ombor) {
  fs.writeFileSync(OMBOR_PATH, JSON.stringify(ombor, null, 2));
}

function findMenuItem(id) {
  return readMenu().find((item) => item.id === id);
}

// ----------------------------------------------------------------------------------
//  OYLIK HISOBOTNI AVTOMAT TEKSHIRISH VA ARXIVLASH FUNKSIYASI
// ----------------------------------------------------------------------------------
function checkMonthRollover(db) {
  const now = new Date();
  const realYear = now.getFullYear();
  const realMonth = now.getMonth() + 1;

  const monthChanged = db.stats.currentYear !== realYear || db.stats.currentMonth !== realMonth;

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

// Taom/ingredient nomidan URL-friendly ID generatsiya qilish
function slugify(text) {
  const translit = { 'ў': 'u', 'қ': 'q', 'ғ': 'gh', 'ҳ': 'h', 'ш': 'sh', 'ч': 'ch', "'": '' };
  let result = text.toLowerCase();
  Object.keys(translit).forEach((k) => { result = result.split(k).join(translit[k]); });
  result = result.replace(/[^a-z0-9\s-]/g, '').trim().replace(/\s+/g, '-');
  return result || 'item';
}

function generateMenuItemId(name, menu) {
  const base = slugify(name);
  let id = base;
  let counter = 2;
  while (menu.find((item) => item.id === id)) {
    id = base + '-' + counter;
    counter++;
  }
  return id;
}

function generateIngredientId(name, ombor) {
  const base = slugify(name);
  let id = base;
  let counter = 2;
  while (ombor.find((i) => i.id === id)) {
    id = base + '-' + counter;
    counter++;
  }
  return id;
}

function generateOrderId() {
  return 'ORD-' + Date.now() + '-' + Math.floor(Math.random() * 1000);
}

const LOW_STOCK_THRESHOLD = 20;

// Buyurtma to'langanda, taomlar retseptiga qarab ombordan avtomat ayirib,
// sotilgan sonini oshiradigan funksiya
function applyOrderToOmbor(orderItems) {
  const ombor = readOmbor();
  const menu = readMenu();

  for (const orderItem of orderItems) {
    const menuItem = menu.find((m) => m.id === orderItem.id);
    if (!menuItem || !Array.isArray(menuItem.recipe)) continue;

    for (const recipeLine of menuItem.recipe) {
      const ingredient = ombor.find((i) => i.id === recipeLine.ingredientId);
      if (!ingredient) continue;
      const consumed = (recipeLine.qty || 0) * orderItem.qty;
      ingredient.stock = ingredient.stock - consumed;
      ingredient.totalSold = (ingredient.totalSold || 0) + consumed;
    }
  }

  writeOmbor(ombor);
  return ombor;
}

// ==================================================================================
//  ROUTE'LAR
// ==================================================================================

app.get('/menu', (req, res) => {
  res.sendFile(__dirname + '/index.html');
});

app.get('/admin', (req, res) => {
  res.sendFile(__dirname + '/admin.html');
});

app.get('/', (req, res) => {
  res.redirect('/menu?table=1');
});

// --------------------------- API: MENYUNI OLISH (mijoz uchun) ---------------------------
app.get('/api/menu', (req, res) => {
  const publicMenu = readMenu().map(({ id, category, name, price, emoji, description, image }) => ({
    id, category, name, price, emoji, description, image,
  }));
  res.json(publicMenu);
});

// =====================================================================================
//  ADMIN UCHUN MENYU BOSHQARUVI
// =====================================================================================

app.get('/api/admin/menu', (req, res) => {
  res.json(readMenu());
});

app.post('/api/admin/menu', (req, res) => {
  const { category, name, price, cost, emoji, description, image, recipe } = req.body;

  if (!category || !name || price === undefined) {
    return res.status(400).json({ error: 'Kategoriya, nom va narx to\'ldirilishi shart.' });
  }

  const menu = readMenu();
  const newItem = {
    id: generateMenuItemId(name, menu),
    category: String(category).trim(),
    name: String(name).trim(),
    price: Math.max(0, parseInt(price, 10) || 0),
    cost: Math.max(0, parseInt(cost, 10) || 0),
    emoji: emoji && String(emoji).trim() ? String(emoji).trim() : '🍽️',
    description: description ? String(description).trim() : '',
    image: image ? String(image) : '',
    recipe: Array.isArray(recipe)
      ? recipe.filter((r) => r && r.ingredientId).map((r) => ({ ingredientId: r.ingredientId, qty: Math.max(0, parseFloat(r.qty) || 0) }))
      : [],
  };

  menu.push(newItem);
  writeMenu(menu);
  io.emit('menu-updated');

  res.status(201).json(newItem);
});

app.put('/api/admin/menu/:id', (req, res) => {
  const { id } = req.params;
  const { category, name, price, cost, emoji, description, image, recipe } = req.body;

  const menu = readMenu();
  const item = menu.find((i) => i.id === id);
  if (!item) {
    return res.status(404).json({ error: 'Taom topilmadi.' });
  }

  if (category !== undefined) item.category = String(category).trim();
  if (name !== undefined) item.name = String(name).trim();
  if (price !== undefined) item.price = Math.max(0, parseInt(price, 10) || 0);
  if (cost !== undefined) item.cost = Math.max(0, parseInt(cost, 10) || 0);
  if (emoji !== undefined) item.emoji = String(emoji).trim() || '🍽️';
  if (description !== undefined) item.description = String(description).trim();
  if (image !== undefined) item.image = String(image);
  if (recipe !== undefined) {
    item.recipe = Array.isArray(recipe)
      ? recipe.filter((r) => r && r.ingredientId).map((r) => ({ ingredientId: r.ingredientId, qty: Math.max(0, parseFloat(r.qty) || 0) }))
      : [];
  }

  writeMenu(menu);
  io.emit('menu-updated');

  res.json(item);
});

app.delete('/api/admin/menu/:id', (req, res) => {
  const { id } = req.params;
  const menu = readMenu();
  const index = menu.findIndex((i) => i.id === id);
  if (index === -1) {
    return res.status(404).json({ error: 'Taom topilmadi.' });
  }
  menu.splice(index, 1);
  writeMenu(menu);
  io.emit('menu-updated');

  res.json({ success: true });
});

// --------------------------- API: BUYURTMA YARATISH ---------------------------
app.post('/api/orders', (req, res) => {
  const { table, items } = req.body;

  if (!table) {
    return res.status(400).json({ error: 'Stol raqami ko\'rsatilmagan.' });
  }
  if (!Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: 'Savat bo\'sh. Buyurtma berish uchun kamida bitta taom tanlang.' });
  }

  const orderItems = [];
  let total = 0;
  let totalCost = 0;

  for (const reqItem of items) {
    const menuItem = findMenuItem(reqItem.id);
    if (!menuItem) continue;
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
    // TO'LOV USULI: Hozircha faqat "Naqd pul / Terminal".
    // Kelajakda Click/Payme ulash uchun:
    // // SHU JOYGA O'Z LINKINGIZNI YOZING (Click/Payme API endpoint manzili)
    // -----------------------------------------------------------------
    paymentMethod: 'naqd_terminal',
    status: 'new',
    createdAt: new Date().toISOString(),
    paidAt: null,
  };

  const db = readDB();
  db.orders.push(order);
  writeDB(db);

  io.to('admin-room').emit('new-order', order);
  io.to('table-' + order.table).emit('order-created', order);

  res.status(201).json(order);
});

app.get('/api/orders', (req, res) => {
  const db = readDB();
  const sorted = [...db.orders].sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  res.json(sorted);
});

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

  db = checkMonthRollover(db);

  order.status = 'paid';
  order.paidAt = new Date().toISOString();

  db.stats.totalRevenue += order.total;
  db.stats.totalCost += order.totalCost;
  db.stats.totalProfit = db.stats.totalRevenue - db.stats.totalCost;

  writeDB(db);

  const updatedOmbor = applyOrderToOmbor(order.items);
  const lowStockItems = updatedOmbor.filter((i) => i.stock <= LOW_STOCK_THRESHOLD);

  io.to('admin-room').emit('order-paid', order);
  io.to('admin-room').emit('stats-update', { stats: db.stats, archive: db.archive });
  io.to('admin-room').emit('ombor-updated', { ombor: updatedOmbor, lowStockItems });
  io.to('table-' + order.table).emit('order-paid', order);

  res.json(order);
});

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

app.get('/api/stats', (req, res) => {
  let db = readDB();
  db = checkMonthRollover(db);
  res.json({ stats: db.stats, archive: db.archive });
});

// =====================================================================================
//  ADMIN UCHUN OMBOR (INVENTAR) BOSHQARUVI
// =====================================================================================

app.get('/api/admin/ombor', (req, res) => {
  const ombor = readOmbor();
  res.json({ ombor, lowStockThreshold: LOW_STOCK_THRESHOLD });
});

app.post('/api/admin/ombor', (req, res) => {
  const { name, unit, stock } = req.body;
  if (!name) {
    return res.status(400).json({ error: 'Ingredient nomini kiriting.' });
  }
  const ombor = readOmbor();
  const newIngredient = {
    id: generateIngredientId(name, ombor),
    name: String(name).trim(),
    unit: unit ? String(unit).trim() : 'dona',
    stock: Math.max(0, parseFloat(stock) || 0),
    totalSold: 0,
  };
  ombor.push(newIngredient);
  writeOmbor(ombor);
  io.to('admin-room').emit('ombor-updated', {
    ombor,
    lowStockItems: ombor.filter((i) => i.stock <= LOW_STOCK_THRESHOLD),
  });
  res.status(201).json(newIngredient);
});

app.post('/api/admin/ombor/:id/add-stock', (req, res) => {
  const { id } = req.params;
  const { amount } = req.body;
  const ombor = readOmbor();
  const ingredient = ombor.find((i) => i.id === id);
  if (!ingredient) {
    return res.status(404).json({ error: 'Ingredient topilmadi.' });
  }
  ingredient.stock += Math.max(0, parseFloat(amount) || 0);
  writeOmbor(ombor);
  io.to('admin-room').emit('ombor-updated', {
    ombor,
    lowStockItems: ombor.filter((i) => i.stock <= LOW_STOCK_THRESHOLD),
  });
  res.json(ingredient);
});

app.put('/api/admin/ombor/:id', (req, res) => {
  const { id } = req.params;
  const { name, unit, stock } = req.body;
  const ombor = readOmbor();
  const ingredient = ombor.find((i) => i.id === id);
  if (!ingredient) {
    return res.status(404).json({ error: 'Ingredient topilmadi.' });
  }
  if (name !== undefined) ingredient.name = String(name).trim();
  if (unit !== undefined) ingredient.unit = String(unit).trim();
  if (stock !== undefined) ingredient.stock = Math.max(0, parseFloat(stock) || 0);
  writeOmbor(ombor);
  io.to('admin-room').emit('ombor-updated', {
    ombor,
    lowStockItems: ombor.filter((i) => i.stock <= LOW_STOCK_THRESHOLD),
  });
  res.json(ingredient);
});

app.delete('/api/admin/ombor/:id', (req, res) => {
  const { id } = req.params;
  const ombor = readOmbor();
  const index = ombor.findIndex((i) => i.id === id);
  if (index === -1) {
    return res.status(404).json({ error: 'Ingredient topilmadi.' });
  }
  ombor.splice(index, 1);
  writeOmbor(ombor);
  io.to('admin-room').emit('ombor-updated', {
    ombor,
    lowStockItems: ombor.filter((i) => i.stock <= LOW_STOCK_THRESHOLD),
  });
  res.json({ success: true });
});

// ==================================================================================
//  SOCKET.IO — REAL-TIME ALOQA
// ==================================================================================
io.on('connection', (socket) => {
  console.log('Yangi ulanish:', socket.id);

  socket.on('join-admin', () => {
    socket.join('admin-room');
  });

  socket.on('join-table', (table) => {
    socket.join('table-' + table);
  });

  socket.on('disconnect', () => {
    console.log('Ulanish uzildi:', socket.id);
  });
});

// ----------------------------------------------------------------------------------
//  HAR SOATDA AVTOMAT OYLIK ARXIV TEKSHIRUVI
// ----------------------------------------------------------------------------------
setInterval(() => {
  let db = readDB();
  const before = JSON.stringify(db.stats);
  db = checkMonthRollover(db);
  const after = JSON.stringify(db.stats);
  if (before !== after) {
    io.to('admin-room').emit('stats-update', { stats: db.stats, archive: db.archive });
  }
}, 60 * 60 * 1000);

server.listen(PORT, () => {
  console.log('==============================================');
  console.log(`  FAST-FOOD QR BUYURTMA TIZIMI ISHGA TUSHDI`);
  console.log(`  Ma'lumotlar papkasi: ${DATA_DIR}`);
  console.log(`  Mijoz menyusi:  http://localhost:${PORT}/menu?table=4`);
  console.log(`  Kassa paneli:   http://localhost:${PORT}/admin`);
  console.log('==============================================');
});
