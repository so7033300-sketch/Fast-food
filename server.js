// ==================================================================================
//  FAST-FOOD QR BUYURTMA TIZIMI — BACKEND SERVER
//  Node.js + Express + Socket.io + MongoDB (yoki JSON fayl-baza — avtomat tanlanadi)
//
//  MUHIM: Agar muhit o'zgaruvchisi MONGODB_URI mavjud bo'lsa (masalan Render'da
//  Environment bo'limida sozlangan bo'lsa), tizim MongoDB'ni ishlatadi — bu holda
//  ma'lumotlar HECH QACHON yo'qolmaydi (server qayta ishga tushsa ham).
//  Agar MONGODB_URI YO'Q bo'lsa (masalan lokal kompyuteringizda sinab ko'rayotganda),
//  tizim avtomat ravishda oddiy JSON-fayllarga (db.json, menu.json, ombor.json)
//  yozib turadi — bu holda qo'shimcha sozlashsiz darhol ishga tushadi.
//
//  Ushbu server quyidagilarni ta'minlaydi:
//   1) Mijoz uchun QR-menyu sahifasini va menyu ma'lumotlarini taqdim etadi.
//   2) Mijoz buyurtma bergach, kassa (admin) paneliga real-time (WebSocket) xabar yuboradi.
//   3) Kassir to'lovni tasdiqlagach, statistika (tushum/tannarx/foyda) avtomat yangilanadi.
//   4) Har oy boshlanganda joriy oy statistikasi avtomat arxivga o'tkaziladi va 0'dan boshlanadi.
//   5) Ombordan (inventar) avtomat ayirish va kam qolganda ogohlantirish.
// ==================================================================================

const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const { Server } = require('socket.io');
const { MongoClient } = require('mongodb');

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
//  MA'LUMOTLAR BAZASI — MongoDB (agar MONGODB_URI bo'lsa) yoki JSON fayllar
//
//  MongoDB ulanish manzilini shu yerga emas, balki hosting-provayderingizning
//  "Environment Variables" bo'limiga MONGODB_URI nomi bilan qo'shing:
//
//  // SHU JOYGA O'Z LINKINGIZNI YOZING — MONGODB_URI muhit o'zgaruvchisi sifatida
//  // (masalan: mongodb+srv://user:parol@cluster.xxxxx.mongodb.net/?retryWrites=true&w=majority)
// ==================================================================================

const MONGODB_URI = process.env.MONGODB_URI;
const USE_MONGO = !!MONGODB_URI;

let mongoClient = null;
let mongoDb = null;

async function connectMongo() {
  mongoClient = new MongoClient(MONGODB_URI);
  await mongoClient.connect();
  mongoDb = mongoClient.db('fastfood');
  console.log('✅ MongoDB\'ga muvaffaqiyatli ulandi. Ma\'lumotlar endi doimiy saqlanadi.');
}

// ---------------------------------------------------------------------
// JSON-FAYL REJIMI (MONGODB_URI bo'lmaganda ishlatiladigan zaxira usul)
// ---------------------------------------------------------------------
const DB_PATH = path.join(__dirname, 'db.json');
const MENU_PATH = path.join(__dirname, 'menu.json');
const OMBOR_PATH = path.join(__dirname, 'ombor.json');

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

function readDBFile() {
  if (!fs.existsSync(DB_PATH)) {
    const initial = { orders: [], stats: getDefaultStats(), archive: [] };
    fs.writeFileSync(DB_PATH, JSON.stringify(initial, null, 2));
    return initial;
  }
  try {
    return JSON.parse(fs.readFileSync(DB_PATH, 'utf-8'));
  } catch (e) {
    const initial = { orders: [], stats: getDefaultStats(), archive: [] };
    fs.writeFileSync(DB_PATH, JSON.stringify(initial, null, 2));
    return initial;
  }
}
function writeDBFile(data) {
  fs.writeFileSync(DB_PATH, JSON.stringify(data, null, 2));
}

function readMenuFile() {
  if (!fs.existsSync(MENU_PATH)) {
    fs.writeFileSync(MENU_PATH, JSON.stringify([], null, 2));
    return [];
  }
  try {
    return JSON.parse(fs.readFileSync(MENU_PATH, 'utf-8'));
  } catch (e) {
    return [];
  }
}
function writeMenuFile(menu) {
  fs.writeFileSync(MENU_PATH, JSON.stringify(menu, null, 2));
}

function readOmborFile() {
  if (!fs.existsSync(OMBOR_PATH)) {
    fs.writeFileSync(OMBOR_PATH, JSON.stringify([], null, 2));
    return [];
  }
  try {
    return JSON.parse(fs.readFileSync(OMBOR_PATH, 'utf-8'));
  } catch (e) {
    return [];
  }
}
function writeOmborFile(ombor) {
  fs.writeFileSync(OMBOR_PATH, JSON.stringify(ombor, null, 2));
}

// ==================================================================================
//  UNIVERSAL MA'LUMOTLAR FUNKSIYALARI
//  Bu funksiyalar MongoDB yoki JSON-fayl orasida AVTOMAT tanlaydi (USE_MONGO ga qarab).
//  Route'lar (pastda) faqat shu funksiyalarni chaqiradi — qaysi baza ishlatilayotganini
//  bilishi shart emas.
// ==================================================================================

// ---- MENYU ----
async function getMenu() {
  if (USE_MONGO) return mongoDb.collection('menu').find({}).toArray();
  return readMenuFile();
}
async function getMenuItemById(id) {
  if (USE_MONGO) return mongoDb.collection('menu').findOne({ id });
  return readMenuFile().find((i) => i.id === id);
}
async function insertMenuItem(item) {
  if (USE_MONGO) {
    await mongoDb.collection('menu').insertOne(item);
  } else {
    const menu = readMenuFile();
    menu.push(item);
    writeMenuFile(menu);
  }
  return item;
}
async function updateMenuItemById(id, fields) {
  if (USE_MONGO) {
    await mongoDb.collection('menu').updateOne({ id }, { $set: fields });
    return mongoDb.collection('menu').findOne({ id });
  }
  const menu = readMenuFile();
  const item = menu.find((i) => i.id === id);
  if (!item) return null;
  Object.assign(item, fields);
  writeMenuFile(menu);
  return item;
}
async function deleteMenuItemById(id) {
  if (USE_MONGO) {
    const r = await mongoDb.collection('menu').deleteOne({ id });
    return r.deletedCount > 0;
  }
  const menu = readMenuFile();
  const idx = menu.findIndex((i) => i.id === id);
  if (idx === -1) return false;
  menu.splice(idx, 1);
  writeMenuFile(menu);
  return true;
}

// ---- OMBOR ----
const LOW_STOCK_THRESHOLD = 20;

async function getOmbor() {
  if (USE_MONGO) return mongoDb.collection('ombor').find({}).toArray();
  return readOmborFile();
}
async function insertIngredient(ingredient) {
  if (USE_MONGO) {
    await mongoDb.collection('ombor').insertOne(ingredient);
  } else {
    const ombor = readOmborFile();
    ombor.push(ingredient);
    writeOmborFile(ombor);
  }
  return ingredient;
}
async function addStockToIngredientById(id, amount) {
  if (USE_MONGO) {
    await mongoDb.collection('ombor').updateOne({ id }, { $inc: { stock: amount } });
    return mongoDb.collection('ombor').findOne({ id });
  }
  const ombor = readOmborFile();
  const ing = ombor.find((i) => i.id === id);
  if (!ing) return null;
  ing.stock += amount;
  writeOmborFile(ombor);
  return ing;
}
async function updateIngredientById(id, fields) {
  if (USE_MONGO) {
    await mongoDb.collection('ombor').updateOne({ id }, { $set: fields });
    return mongoDb.collection('ombor').findOne({ id });
  }
  const ombor = readOmborFile();
  const ing = ombor.find((i) => i.id === id);
  if (!ing) return null;
  Object.assign(ing, fields);
  writeOmborFile(ombor);
  return ing;
}
async function deleteIngredientById(id) {
  if (USE_MONGO) {
    const r = await mongoDb.collection('ombor').deleteOne({ id });
    return r.deletedCount > 0;
  }
  const ombor = readOmborFile();
  const idx = ombor.findIndex((i) => i.id === id);
  if (idx === -1) return false;
  ombor.splice(idx, 1);
  writeOmborFile(ombor);
  return true;
}

// Buyurtma to'langanda, sotilgan taomlar retseptiga qarab ombordan avtomat ayirish
async function applyOrderToOmbor(orderItems) {
  const menu = await getMenu();
  for (const orderItem of orderItems) {
    const menuItem = menu.find((m) => m.id === orderItem.id);
    if (!menuItem || !Array.isArray(menuItem.recipe)) continue;

    for (const line of menuItem.recipe) {
      const consumed = (line.qty || 0) * orderItem.qty;
      if (consumed <= 0) continue;

      if (USE_MONGO) {
        await mongoDb
          .collection('ombor')
          .updateOne({ id: line.ingredientId }, { $inc: { stock: -consumed, totalSold: consumed } });
      } else {
        const ombor = readOmborFile();
        const ing = ombor.find((i) => i.id === line.ingredientId);
        if (ing) {
          ing.stock -= consumed;
          ing.totalSold = (ing.totalSold || 0) + consumed;
          writeOmborFile(ombor);
        }
      }
    }
  }
  return getOmbor();
}

// ---- BUYURTMALAR ----
async function getOrders() {
  if (USE_MONGO) return mongoDb.collection('orders').find({}).toArray();
  return readDBFile().orders;
}
async function getOrderById(id) {
  if (USE_MONGO) return mongoDb.collection('orders').findOne({ id });
  return readDBFile().orders.find((o) => o.id === id);
}
async function insertOrder(order) {
  if (USE_MONGO) {
    await mongoDb.collection('orders').insertOne(order);
  } else {
    const dbFile = readDBFile();
    dbFile.orders.push(order);
    writeDBFile(dbFile);
  }
  return order;
}
async function updateOrderById(id, fields) {
  if (USE_MONGO) {
    await mongoDb.collection('orders').updateOne({ id }, { $set: fields });
  } else {
    const dbFile = readDBFile();
    const order = dbFile.orders.find((o) => o.id === id);
    if (order) Object.assign(order, fields);
    writeDBFile(dbFile);
  }
}

// ---- STATISTIKA VA ARXIV ----
async function getStatsAndArchive() {
  if (USE_MONGO) {
    let doc = await mongoDb.collection('meta').findOne({ _id: 'app' });
    if (!doc) {
      doc = { _id: 'app', stats: getDefaultStats(), archive: [] };
      await mongoDb.collection('meta').insertOne(doc);
    }
    return { stats: doc.stats, archive: doc.archive };
  }
  const dbFile = readDBFile();
  return { stats: dbFile.stats, archive: dbFile.archive };
}
async function saveStatsAndArchive(stats, archive) {
  if (USE_MONGO) {
    await mongoDb.collection('meta').updateOne({ _id: 'app' }, { $set: { stats, archive } }, { upsert: true });
  } else {
    const dbFile = readDBFile();
    dbFile.stats = stats;
    dbFile.archive = archive;
    writeDBFile(dbFile);
  }
}

// ----------------------------------------------------------------------------------
//  OYLIK HISOBOTNI AVTOMAT TEKSHIRISH VA ARXIVLASH FUNKSIYASI
// ----------------------------------------------------------------------------------
async function checkMonthRollover() {
  const { stats, archive } = await getStatsAndArchive();
  const now = new Date();
  const realYear = now.getFullYear();
  const realMonth = now.getMonth() + 1;

  const monthChanged = stats.currentYear !== realYear || stats.currentMonth !== realMonth;

  if (monthChanged) {
    const hadActivity = stats.totalRevenue > 0 || stats.totalCost > 0;
    if (hadActivity) {
      archive.unshift({
        year: stats.currentYear,
        month: stats.currentMonth,
        totalRevenue: stats.totalRevenue,
        totalCost: stats.totalCost,
        totalProfit: stats.totalProfit,
        archivedAt: now.toISOString(),
      });
    }
    stats.currentYear = realYear;
    stats.currentMonth = realMonth;
    stats.totalRevenue = 0;
    stats.totalCost = 0;
    stats.totalProfit = 0;

    await saveStatsAndArchive(stats, archive);
    console.log(`[OYLIK ARXIV] Yangi oy aniqlandi: ${realMonth}/${realYear}. Statistika 0 ga tushirildi.`);
  }

  return { stats, archive };
}

// Taom/ingredient nomidan URL-friendly ID generatsiya qilish
function slugify(text) {
  const translit = { 'ў': 'u', 'қ': 'q', 'ғ': 'gh', 'ҳ': 'h', 'ш': 'sh', 'ч': 'ch', "'": '' };
  let result = text.toLowerCase();
  Object.keys(translit).forEach((k) => { result = result.split(k).join(translit[k]); });
  result = result.replace(/[^a-z0-9\s-]/g, '').trim().replace(/\s+/g, '-');
  return result || 'item';
}

async function generateMenuItemId(name) {
  const menu = await getMenu();
  const base = slugify(name);
  let id = base;
  let counter = 2;
  while (menu.find((item) => item.id === id)) {
    id = base + '-' + counter;
    counter++;
  }
  return id;
}

async function generateIngredientId(name) {
  const ombor = await getOmbor();
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
app.get('/api/menu', async (req, res) => {
  try {
    const menu = await getMenu();
    const publicMenu = menu.map(({ id, category, name, price, emoji, description, image }) => ({
      id, category, name, price, emoji, description, image,
    }));
    res.json(publicMenu);
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

// =====================================================================================
//  ADMIN UCHUN MENYU BOSHQARUVI
// =====================================================================================

app.get('/api/admin/menu', async (req, res) => {
  try {
    res.json(await getMenu());
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

app.post('/api/admin/menu', async (req, res) => {
  try {
    const { category, name, price, cost, emoji, description, image, recipe } = req.body;
    if (!category || !name || price === undefined) {
      return res.status(400).json({ error: 'Kategoriya, nom va narx to\'ldirilishi shart.' });
    }

    const newItem = {
      id: await generateMenuItemId(name),
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

    await insertMenuItem(newItem);
    io.emit('menu-updated');
    res.status(201).json(newItem);
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

app.put('/api/admin/menu/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { category, name, price, cost, emoji, description, image, recipe } = req.body;

    const fields = {};
    if (category !== undefined) fields.category = String(category).trim();
    if (name !== undefined) fields.name = String(name).trim();
    if (price !== undefined) fields.price = Math.max(0, parseInt(price, 10) || 0);
    if (cost !== undefined) fields.cost = Math.max(0, parseInt(cost, 10) || 0);
    if (emoji !== undefined) fields.emoji = String(emoji).trim() || '🍽️';
    if (description !== undefined) fields.description = String(description).trim();
    if (image !== undefined) fields.image = String(image);
    if (recipe !== undefined) {
      fields.recipe = Array.isArray(recipe)
        ? recipe.filter((r) => r && r.ingredientId).map((r) => ({ ingredientId: r.ingredientId, qty: Math.max(0, parseFloat(r.qty) || 0) }))
        : [];
    }

    const updated = await updateMenuItemById(id, fields);
    if (!updated) return res.status(404).json({ error: 'Taom topilmadi.' });

    io.emit('menu-updated');
    res.json(updated);
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

app.delete('/api/admin/menu/:id', async (req, res) => {
  try {
    const ok = await deleteMenuItemById(req.params.id);
    if (!ok) return res.status(404).json({ error: 'Taom topilmadi.' });
    io.emit('menu-updated');
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

// --------------------------- API: BUYURTMA YARATISH ---------------------------
app.post('/api/orders', async (req, res) => {
  try {
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
      const menuItem = await getMenuItemById(reqItem.id);
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
      // TO'LOV USULI: Hozircha faqat "Naqd pul / Terminal" (kassada to'lash).
      // Kelajakda Click yoki Payme kabi onlayn to'lov tizimlarini ulash uchun:
      // // SHU JOYGA O'Z LINKINGIZNI YOZING (Click/Payme API endpoint manzili)
      // -----------------------------------------------------------------
      paymentMethod: 'naqd_terminal',
      status: 'new',
      createdAt: new Date().toISOString(),
      paidAt: null,
    };

    await insertOrder(order);

    io.to('admin-room').emit('new-order', order);
    io.to('table-' + order.table).emit('order-created', order);

    res.status(201).json(order);
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

app.get('/api/orders', async (req, res) => {
  try {
    const orders = await getOrders();
    const sorted = [...orders].sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json(sorted);
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

app.post('/api/orders/:id/pay', async (req, res) => {
  try {
    const { id } = req.params;
    const order = await getOrderById(id);
    if (!order) return res.status(404).json({ error: 'Buyurtma topilmadi.' });
    if (order.status === 'paid') return res.status(400).json({ error: 'Bu buyurtma allaqachon to\'langan.' });

    const { stats, archive } = await checkMonthRollover();

    const paidAt = new Date().toISOString();
    stats.totalRevenue += order.total;
    stats.totalCost += order.totalCost;
    stats.totalProfit = stats.totalRevenue - stats.totalCost;

    await saveStatsAndArchive(stats, archive);
    await updateOrderById(id, { status: 'paid', paidAt });

    const updatedOmbor = await applyOrderToOmbor(order.items);
    const lowStockItems = updatedOmbor.filter((i) => i.stock <= LOW_STOCK_THRESHOLD);

    const paidOrder = { ...order, status: 'paid', paidAt };

    io.to('admin-room').emit('order-paid', paidOrder);
    io.to('admin-room').emit('stats-update', { stats, archive });
    io.to('admin-room').emit('ombor-updated', { ombor: updatedOmbor, lowStockItems });
    io.to('table-' + order.table).emit('order-paid', paidOrder);

    res.json(paidOrder);
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

app.post('/api/orders/:id/cancel', async (req, res) => {
  try {
    const { id } = req.params;
    const order = await getOrderById(id);
    if (!order) return res.status(404).json({ error: 'Buyurtma topilmadi.' });
    if (order.status === 'paid') return res.status(400).json({ error: 'To\'langan buyurtmani bekor qilib bo\'lmaydi.' });

    await updateOrderById(id, { status: 'cancelled' });
    const cancelledOrder = { ...order, status: 'cancelled' };

    io.to('admin-room').emit('order-cancelled', cancelledOrder);
    io.to('table-' + order.table).emit('order-cancelled', cancelledOrder);
    res.json(cancelledOrder);
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

app.get('/api/stats', async (req, res) => {
  try {
    const { stats, archive } = await checkMonthRollover();
    res.json({ stats, archive });
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

// =====================================================================================
//  ADMIN UCHUN OMBOR (INVENTAR) BOSHQARUVI
// =====================================================================================

app.get('/api/admin/ombor', async (req, res) => {
  try {
    const ombor = await getOmbor();
    res.json({ ombor, lowStockThreshold: LOW_STOCK_THRESHOLD });
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

app.post('/api/admin/ombor', async (req, res) => {
  try {
    const { name, unit, stock } = req.body;
    if (!name) return res.status(400).json({ error: 'Ingredient nomini kiriting.' });

    const newIngredient = {
      id: await generateIngredientId(name),
      name: String(name).trim(),
      unit: unit ? String(unit).trim() : 'dona',
      stock: Math.max(0, parseFloat(stock) || 0),
      totalSold: 0,
    };
    await insertIngredient(newIngredient);

    const ombor = await getOmbor();
    io.to('admin-room').emit('ombor-updated', { ombor, lowStockItems: ombor.filter((i) => i.stock <= LOW_STOCK_THRESHOLD) });
    res.status(201).json(newIngredient);
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

app.post('/api/admin/ombor/:id/add-stock', async (req, res) => {
  try {
    const { id } = req.params;
    const { amount } = req.body;
    const updated = await addStockToIngredientById(id, Math.max(0, parseFloat(amount) || 0));
    if (!updated) return res.status(404).json({ error: 'Ingredient topilmadi.' });

    const ombor = await getOmbor();
    io.to('admin-room').emit('ombor-updated', { ombor, lowStockItems: ombor.filter((i) => i.stock <= LOW_STOCK_THRESHOLD) });
    res.json(updated);
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

app.put('/api/admin/ombor/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { name, unit, stock } = req.body;
    const fields = {};
    if (name !== undefined) fields.name = String(name).trim();
    if (unit !== undefined) fields.unit = String(unit).trim();
    if (stock !== undefined) fields.stock = Math.max(0, parseFloat(stock) || 0);

    const updated = await updateIngredientById(id, fields);
    if (!updated) return res.status(404).json({ error: 'Ingredient topilmadi.' });

    const ombor = await getOmbor();
    io.to('admin-room').emit('ombor-updated', { ombor, lowStockItems: ombor.filter((i) => i.stock <= LOW_STOCK_THRESHOLD) });
    res.json(updated);
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
});

app.delete('/api/admin/ombor/:id', async (req, res) => {
  try {
    const ok = await deleteIngredientById(req.params.id);
    if (!ok) return res.status(404).json({ error: 'Ingredient topilmadi.' });

    const ombor = await getOmbor();
    io.to('admin-room').emit('ombor-updated', { ombor, lowStockItems: ombor.filter((i) => i.stock <= LOW_STOCK_THRESHOLD) });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: 'Server xatosi: ' + e.message });
  }
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
setInterval(async () => {
  try {
    const before = await getStatsAndArchive();
    const beforeStr = JSON.stringify(before.stats);
    const after = await checkMonthRollover();
    if (beforeStr !== JSON.stringify(after.stats)) {
      io.to('admin-room').emit('stats-update', after);
    }
  } catch (e) {
    console.error('Oylik tekshiruvda xato:', e);
  }
}, 60 * 60 * 1000);

// ==================================================================================
//  SERVERNI ISHGA TUSHIRISH
// ==================================================================================
async function start() {
  if (USE_MONGO) {
    try {
      await connectMongo();
    } catch (e) {
      console.error('❌ MongoDB\'ga ulanib bo\'lmadi:', e.message);
      console.error('   MONGODB_URI to\'g\'riligini va Network Access sozlamalarini tekshiring.');
      process.exit(1);
    }
  } else {
    console.log('ℹ️  MONGODB_URI topilmadi — JSON-fayl rejimida ishga tushmoqda (lokal test uchun).');
  }

  server.listen(PORT, () => {
    console.log('==============================================');
    console.log(`  FAST-FOOD QR BUYURTMA TIZIMI ISHGA TUSHDI`);
    console.log(`  Baza turi: ${USE_MONGO ? 'MongoDB (doimiy)' : 'JSON-fayl (vaqtinchalik)'}`);
    console.log(`  Mijoz menyusi:  http://localhost:${PORT}/menu?table=4`);
    console.log(`  Kassa paneli:   http://localhost:${PORT}/admin`);
    console.log('==============================================');
  });
}

start();
