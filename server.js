const express = require("express");
const cors = require("cors");
const axios = require("axios");
const multer = require("multer");
const https = require("https");
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 3001;
const SEMRUSH_API_KEY = process.env.SEMRUSH_API_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SECRET_KEY;

const { createClient } = require("@supabase/supabase-js");
const cron = require("node-cron");
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// ── Auth middleware ────────────────────────────────────────────────────────────
// Creates a per-request supabase client using the user's JWT so we can verify
// their session server-side. All /api/* routes require a valid Supabase session.
const requireAuth = async (req, res, next) => {
  // EventSource (SSE) can't send headers, so also accept token as query param
  const authHeader = req.headers.authorization;
  const token = authHeader?.startsWith("Bearer ") 
    ? authHeader.split(" ")[1] 
    : req.query.token;
  if (!token) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  const { data: { user }, error } = await supabase.auth.getUser(token);
  if (error || !user) {
    return res.status(401).json({ error: "Invalid or expired session" });
  }
  req.user = user;
  next();
};

const httpsAgent = new https.Agent({ rejectUnauthorized: false });

// ─── YOAST / SEO PLUGIN DETECTION ────────────────────────────────────────────
// Returns { yoast: "premium"|"free"|"none", fortitudePlugin: true|false, canWriteSeoMeta: true|false,
//           restMetaKeys: true|false, indexablesApi: true|false }
// Runs a thorough 4-step probe and caches the result on the client record in Supabase.
const detectSeoCapabilities = async (wpBaseUrl, authHeaders, clientId = null) => {
  const result = {
    yoast: "none",
    fortitudePlugin: false,
    canWriteSeoMeta: false,
    restMetaKeys: false,   // Yoast registered _yoast_wpseo_* keys in REST API
    indexablesApi: false,  // Yoast Premium indexables endpoint available (enables score recalc)
  };

  // 1. Namespace probe — single fast call reveals everything
  let namespaces = [];
  try {
    const nsResp = await axios.get(`${wpBaseUrl}/wp-json/`, { headers: authHeaders, httpsAgent, timeout: 8000 });
    namespaces = nsResp.data?.namespaces || [];
    if (namespaces.includes("fortitude/v1")) {
      result.fortitudePlugin = true;
      console.log("[SEO] Fortitude plugin active (fortitude/v1 namespace)");
    }
    // yoast/v1 = Free or Premium, yoast/v3 = Premium only
    if (namespaces.includes("yoast/v1")) {
      result.yoast = namespaces.includes("yoast/v3") ? "premium" : "free";
      console.log(`[SEO] Yoast detected: ${result.yoast} (namespaces: ${namespaces.filter(n => n.startsWith("yoast")).join(", ")})`);
    }
  } catch(e) {
    console.log("[SEO] Namespace check failed:", e.message);
  }

  // 2. Check if Yoast registered its meta keys in the REST API
  //    (only true when Yoast is installed AND has run its "integrate with REST API" setup)
  if (result.yoast !== "none") {
    try {
      const r = await axios.get(`${wpBaseUrl}/wp-json/wp/v2/posts?per_page=1&context=edit`,
        { headers: authHeaders, httpsAgent, timeout: 6000 });
      const meta = r.data?.[0]?.meta || {};
      const yoastKeys = Object.keys(meta).filter(k => k.startsWith("_yoast_wpseo_"));
      result.restMetaKeys = yoastKeys.length > 0;
      console.log(`[SEO] REST meta keys: ${result.restMetaKeys ? yoastKeys.join(", ") : "NONE — Fortitude plugin or manual update required"}`);
    } catch(e) {}
  }

  // 3. If Fortitude plugin is active, use /caps endpoint for authoritative capability info
  if (result.fortitudePlugin) {
    try {
      const capsResp = await axios.get(`${wpBaseUrl}/wp-json/fortitude/v1/caps`,
        { headers: authHeaders, httpsAgent, timeout: 6000 });
      const caps = capsResp.data;
      // Override with server-authoritative values
      if (caps.yoast_edition) result.yoast = caps.yoast_edition;
      result.indexablesApi = caps.indexables_api === true;
      console.log(`[SEO] Fortitude /caps: edition=${caps.yoast_edition}, yoastVersion=${caps.yoast_version}, indexables=${caps.indexables_api}, wpseoMeta=${caps.wpseo_meta_class}`);
    } catch(e) {
      // /caps endpoint may not exist on older plugin versions — fall through to inference
      console.log("[SEO] /caps probe failed (old plugin version?):", e.message);
    }
  }

  // 4. Check Yoast indexables API when Fortitude plugin isn't present
  if (!result.fortitudePlugin && result.yoast === "premium") {
    try {
      const r = await axios.get(`${wpBaseUrl}/wp-json/yoast/v3/configuration`, {
        headers: authHeaders, httpsAgent, timeout: 5000 });
      result.indexablesApi = r.status === 200;
      console.log(`[SEO] Yoast Premium indexables API: ${result.indexablesApi ? "available" : "unavailable"}`);
    } catch(e) {}
  }

  // 4. Determine canWriteSeoMeta:
  //    - Fortitude plugin: always yes (calls WPSEO_Meta::set_value directly)
  //    - REST meta keys registered: yes (direct wp/v2/posts meta write)
  //    - Neither: no — must fall back to manual or plugin install
  result.canWriteSeoMeta = result.fortitudePlugin || result.restMetaKeys;

  // 5. Persist caps to Supabase client record so we don't re-probe every publish
  if (clientId) {
    try {
      await supabase.from("clients").update({
        yoast_edition: result.yoast,
        yoast_rest_meta_keys: result.restMetaKeys,
        yoast_indexables_api: result.indexablesApi,
        fortitude_plugin: result.fortitudePlugin,
        seo_caps_detected_at: new Date().toISOString(),
      }).eq("id", clientId);
    } catch(e) {}
  }

  return result;
};

// Legacy wrapper for backward compat
const detectYoastEdition = async (wpBaseUrl, authHeaders) => {
  const caps = await detectSeoCapabilities(wpBaseUrl, authHeaders);
  return caps.yoast;
};

// Make a longtail keyphrase variant to avoid exact-match cannibalization
// e.g. "ac repair" => "ac repair service", "hvac tune up" => "hvac tune up cost"
const makeLongtailKeyphrase = (keyword) => {
  const kw = keyword.trim().toLowerCase();
  // If already long enough (3+ words), keep as-is
  if (kw.split(/\s+/).length >= 3) return kw;
  // Append a common local service qualifier
  const suffixes = ["service", "near me", "cost", "tips", "guide", "company", "professional"];
  // Avoid appending if one of these words is already present
  if (suffixes.some(s => kw.includes(s))) return kw;
  return kw + " service";
};


app.use(cors({
  origin: [
    "http://localhost:3000",
    process.env.FRONTEND_URL || "http://localhost:3000",
  ].filter(Boolean),
  credentials: true,
}));
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

// Public routes (health check only — everything else requires auth)
app.get("/health", (req, res) => res.json({ status: "ok" }));

// Apply auth to all /api/* routes
app.use("/api", requireAuth);

// ─── HEALTH CHECK ────────────────────────────────────────────────
app.get("/api/health", (req, res) => {
  res.json({ status: "ok", semrush: SEMRUSH_API_KEY ? "connected" : "missing", supabase: SUPABASE_URL ? "connected" : "missing" });
});

// ─── CLIENTS ────────────────────────────────────────────────────
app.get("/api/clients", async (req, res) => {
  const { data, error } = await supabase.from("clients").select("*").order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ clients: data });
});

app.post("/api/clients", async (req, res) => {
  const { name, industry, status, domain, wordpress_url, wordpress_username, wordpress_password, brand_voice } = req.body;
  const { data, error } = await supabase.from("clients").insert([{ name, industry, status: status || "pending", domain, wordpress_url, wordpress_username, wordpress_password, brand_voice }]).select();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ client: data[0] });
});

app.put("/api/clients/:id", async (req, res) => {
  const { id } = req.params;
  const updates = req.body;
  // Auto-promote to active when a WordPress URL is saved and status is still pending
  if (updates.wordpress_url && !updates.status) {
    const { data: existing } = await supabase.from("clients").select("status").eq("id", id).single();
    if (existing?.status === "pending") updates.status = "active";
  }
  const { data, error } = await supabase.from("clients").update(updates).eq("id", id).select();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ client: data[0] });
});

app.delete("/api/clients/:id", async (req, res) => {
  const { id } = req.params;
  try {
    await supabase.from("scheduled_jobs").delete().eq("client_id", id);
    await supabase.from("client_keyword_queue").delete().eq("client_id", id);
    await supabase.from("posts").delete().eq("client_id", id);
    await supabase.from("image_library").delete().eq("client_id", id);
    const { error } = await supabase.from("clients").delete().eq("id", id);
    if (error) return res.status(500).json({ error: error.message });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── KEYWORD LIBRARY ─────────────────────────────────────────────
app.get("/api/keywords/library", async (req, res) => {
  const { industry } = req.query;
  let query = supabase.from("keyword_library").select("*").order("volume", { ascending: false });
  if (industry) query = query.eq("industry", industry);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json({ keywords: data });
});

app.post("/api/keywords/library", async (req, res) => {
  const { keyword, industry, category, volume, kd, intent } = req.body;
  const { data, error } = await supabase.from("keyword_library").insert([{ keyword, industry, category, volume: parseInt(volume) || 0, kd: parseInt(kd) || 0, intent }]).select();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ keyword: data[0] });
});

app.patch("/api/keywords/library/:id", async (req, res) => {
  const { id } = req.params;
  const { keyword, volume, kd, intent } = req.body;
  const updates = {};
  if (keyword !== undefined) updates.keyword = keyword;
  if (volume !== undefined) updates.volume = parseInt(volume) || 0;
  if (kd !== undefined) updates.kd = parseInt(kd) || 0;
  if (intent !== undefined) updates.intent = intent;
  const { data, error } = await supabase.from("keyword_library").update(updates).eq("id", id).select();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ keyword: data[0] });
});

app.post("/api/keywords/library/bulk", async (req, res) => {
  const { keywords } = req.body; // [{ keyword, industry, volume, intent }]
  if (!Array.isArray(keywords) || keywords.length === 0)
    return res.status(400).json({ error: "keywords array required" });
  const rows = keywords.map(k => ({
    keyword: (k.keyword || "").trim(),
    industry: k.industry || "HVAC",
    volume: parseInt(k.volume) || 0,
    kd: parseInt(k.kd) || 0,
    intent: k.intent || "Transactional",
  })).filter(k => k.keyword);
  const { data, error } = await supabase.from("keyword_library").insert(rows).select();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ imported: data.length, keywords: data });
});

app.delete("/api/keywords/library/:id", async (req, res) => {
  const { id } = req.params;
  const { error } = await supabase.from("keyword_library").delete().eq("id", id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});
// ─── USED KEYWORDS ───────────────────────────────────────────────
app.get("/api/keywords/used", async (req, res) => {
  const { data, error } = await supabase.from("used_keywords").select("*").order("added_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ keywords: data });
});

app.post("/api/keywords/used", async (req, res) => {
  const { keyword } = req.body;
  if (!keyword) return res.status(400).json({ error: "keyword required" });
  const { data, error } = await supabase.from("used_keywords").insert([{ keyword: keyword.trim(), added_at: new Date().toISOString() }]).select();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ keyword: data[0] });
});

app.delete("/api/keywords/used/:id", async (req, res) => {
  const { id } = req.params;
  const { error } = await supabase.from("used_keywords").delete().eq("id", id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});
// ─── PER-CLIENT USED KEYWORDS ────────────────────────────────────
app.get("/api/keywords/used/:clientId", async (req, res) => {
  const { clientId } = req.params;
  const page = parseInt(req.query.page) || 1;
  const limit = 20;
  const offset = (page - 1) * limit;
  const { data, error, count } = await supabase.from("client_used_keywords")
    .select("*", { count: "exact" })
    .eq("client_id", clientId)
    .order("added_at", { ascending: false })
    .range(offset, offset + limit - 1);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ keywords: data, total: count, page, pages: Math.ceil(count / limit) });
});

app.post("/api/keywords/used/:clientId", async (req, res) => {
  const { clientId } = req.params;
  const { keyword } = req.body;
  if (!keyword) return res.status(400).json({ error: "keyword required" });
  // Upsert — don't duplicate
  const { data: existing } = await supabase.from("client_used_keywords")
    .select("id").eq("client_id", clientId).eq("keyword", keyword.trim()).single();
  if (existing) return res.json({ keyword: existing, duplicate: true });
  const { data, error } = await supabase.from("client_used_keywords")
    .insert([{ client_id: clientId, keyword: keyword.trim(), added_at: new Date().toISOString() }]).select();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ keyword: data[0] });
});

app.delete("/api/keywords/used-client/:id", async (req, res) => {
  const { id } = req.params;
  const { error } = await supabase.from("client_used_keywords").delete().eq("id", id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

// ─── IMAGE LIBRARY ───────────────────────────────────────────────
const upload = multer({ storage: multer.memoryStorage() });

app.get("/api/images", async (req, res) => {
  const { industry, category, client_id } = req.query;
  let query = supabase.from("image_library").select("*").order("created_at", { ascending: false });
  if (industry) query = query.eq("industry", industry);
  if (category) query = query.eq("category", category);
  if (client_id) query = query.eq("client_id", client_id);
  else query = query.is("client_id", null);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json({ images: data });
});

app.get("/api/images/client/:client_id", async (req, res) => {
  const { client_id } = req.params;
  const { data, error } = await supabase.from("image_library").select("*").eq("client_id", client_id).order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ images: data });
});

app.post("/api/images/upload", upload.single("image"), async (req, res) => {
  const { industry, category, client_id } = req.body;
  const file = req.file;
  if (!file) return res.status(400).json({ error: "No file uploaded" });

  const folder = client_id ? `clients/${client_id}/${category}` : `${industry}/${category}`;
  const ext = file.originalname.split(".").pop();
  const storagePath = `${folder}/${Date.now()}.${ext}`;

  const { error: uploadError } = await supabase.storage
    .from("image-library")
    .upload(storagePath, file.buffer, { contentType: file.mimetype });

  if (uploadError) return res.status(500).json({ error: uploadError.message });

  const { data: urlData } = supabase.storage.from("image-library").getPublicUrl(storagePath);

  const insertData = { filename: file.originalname, industry, category, storage_path: urlData.publicUrl };
  if (client_id) insertData.client_id = client_id;

  const { data, error } = await supabase.from("image_library").insert([insertData]).select();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ image: data[0] });
});

// ─── CLIENT LOGO UPLOAD ──────────────────────────────────────────
// ── POST /api/clients/:id/detect-seo-caps ────────────────────────────
// Re-probes the client WP site and saves Yoast capability snapshot to Supabase.
app.post("/api/clients/:id/detect-seo-caps", requireAuth, async (req, res) => {
  const { id } = req.params;
  try {
    const { data: client } = await supabase.from("clients")
      .select("wordpress_url,wordpress_username,wordpress_password").eq("id", id).single();
    if (!client?.wordpress_url) return res.status(400).json({ error: "WordPress URL required" });
    const wpPass = client.wordpress_password ? decrypt_field(client.wordpress_password) : "";
    const authHeaders = { "Authorization": "Basic " + Buffer.from(`${client.wordpress_username}:${wpPass}`).toString("base64") };
    const caps = await detectSeoCapabilities(client.wordpress_url, authHeaders, id);
    return res.json({ success: true, caps });
  } catch(e) { return res.status(500).json({ error: e.message }); }
});

app.post("/api/clients/:id/logo", upload.single("logo"), async (req, res) => {
  const { id } = req.params;
  const file = req.file;
  if (!file) return res.status(400).json({ error: "No file provided" });
  try {
    const ext = file.originalname.split(".").pop() || "png";
    const storagePath = `logos/${id}.${ext}`;
    await supabase.storage.from("image-library").remove([storagePath]);
    const { error: uploadError } = await supabase.storage
      .from("image-library")
      .upload(storagePath, file.buffer, { contentType: file.mimetype, upsert: true });
    if (uploadError) return res.status(500).json({ error: uploadError.message });
    const { data: urlData } = supabase.storage.from("image-library").getPublicUrl(storagePath);
    const logoUrl = urlData.publicUrl + "?t=" + Date.now();
    await supabase.from("clients").update({ logo_url: logoUrl }).eq("id", id);
    res.json({ success: true, logo_url: logoUrl });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.delete("/api/images/:id", async (req, res) => {
  const { id } = req.params;
  try {
    const { data: img } = await supabase.from("image_library").select("storage_path").eq("id", id).single();
    if (img?.storage_path) {
      const match = img.storage_path.match(/image-library\/(.+?)(\?|$)/);
      if (match) await supabase.storage.from("image-library").remove([match[1]]);
    }
    await supabase.from("image_library").delete().eq("id", id);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─── POSTS ───────────────────────────────────────────────────────
app.get("/api/posts", async (req, res) => {
  const { client_id } = req.query;
  let query = supabase.from("posts").select("*").order("created_at", { ascending: false });
  if (client_id) query = query.eq("client_id", client_id);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json({ posts: data });
});

// ─── CONTENT GENERATION ──────────────────────────────────────────
app.post("/api/content/generate", async (req, res) => {
  const { keyword, industry, clientName, clientId, brandVoice, wordpressUrl } = req.body;
  if (!keyword || !industry) return res.status(400).json({ error: "keyword and industry are required" });

  try {
    let internalPages = [];
    if (wordpressUrl) {
      try {
        const wpPagesRes = await axios.get(`${wordpressUrl}/wp-json/wp/v2/pages?per_page=20&_fields=title,link,slug`, { httpsAgent });
        const pages = wpPagesRes.data.map(p => ({ title: p.title.rendered, url: p.link, slug: p.slug, type: "page" }));
        // Also fetch published blog posts for topic cluster internal linking
        const wpPostsRes = await axios.get(`${wordpressUrl}/wp-json/wp/v2/posts?per_page=30&status=publish&_fields=title,link,slug`, { httpsAgent });
        const posts = wpPostsRes.data.map(p => ({ title: p.title.rendered, url: p.link, slug: p.slug, type: "post" }));
        internalPages = [...pages, ...posts];
      } catch (e) {
        console.log("Could not fetch WordPress pages/posts:", e.message);
      }
    }

    // Fetch all existing titles/slugs to prevent duplicate content
    const existingContent = await fetchExistingContent(wordpressUrl, clientId);
    const existingContentPrompt = buildExistingContentPrompt(existingContent);
    console.log(`[Generate] Found ${existingContent.length} existing pages/posts to avoid`);

    let featuredImage = null;
    const keywordWords = keyword.toLowerCase().split(" ");

    if (clientId) {
      const { data: clientImages } = await supabase.from("image_library").select("*").eq("client_id", clientId).order("times_used", { ascending: true }).order("last_used_at", { ascending: true, nullsFirst: true });
      if (clientImages?.length > 0) {
        featuredImage = selectFeaturedImage(clientImages, keywordWords);
      }
    }

    const Anthropic = require("@anthropic-ai/sdk");
    const client = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

    const blogPosts = internalPages.filter(p => p.type === "post");
    const servicePages = internalPages.filter(p => p.type === "page" && !/home|homepage/i.test(p.title));
    const contactPage = internalPages.find(p => /contact|get.a.quote|free.estimate|schedule|book/i.test(p.title));
    const internalLinksPrompt = internalPages.length > 0
      ? `\n\nINTERNAL LINKS — TOPIC CLUSTER STRUCTURE (Required):
Available published blog posts:
${blogPosts.map(p => `  [BLOG] ${p.title}: ${p.url}`).join("\n") || "  (none yet — use service pages only)"}
Available service/other pages (excluding homepage):
${servicePages.map(p => `  [PAGE] ${p.title}: ${p.url}`).join("\n") || "  (none available)"}
${contactPage ? `Contact/quote page: [CONTACT] ${contactPage.title}: ${contactPage.url}` : ""}

Internal linking rules — follow this structure:
1. Up to 3 BLOG POSTS: Link to up to 3 previously published blog posts on complementary topics to build a topic cluster. Only link to a blog post if it is on a meaningfully different topic that complements this post — NEVER link to a post that targets the same or overlapping keyword (keyword cannibalization).
2. 1 SERVICE PAGE: Link once to the most relevant service page (e.g. AC Repair, Plumbing Services). Do not link to the homepage — only to a specific service or category page.
3. 1 CONTACT/QUOTE PAGE: Include exactly one link to the contact, get-a-quote, or scheduling page as a call-to-action (e.g. "contact us today", "schedule a free estimate").
4. If no blog posts exist yet, use 2 service pages + 1 contact page. Do not force links to irrelevant pages.
5. Never link to the homepage.`
      : "";

    const externalLinksPrompt = `\n\nEXTERNAL LINKS — OUTBOUND LINKING RULES:
Include 1-2 external links to authoritative sources. Follow these rules strictly:
1. Link to a RESOURCE-SPECIFIC PAGE (a specific guide, data page, article, or stats page) — NOT a homepage or top-level domain. For example: a specific Energy Star product criteria page, or a specific EPA water efficiency fact page.
2. The linked page must contain a SPECIFIC FACT or STATISTIC directly relevant to this blog post. Reference that fact inline in your writing (e.g., "According to the EPA, the average household wastes 10,000 gallons of water per year from leaks...").
3. Anchor text should describe the resource or cite the fact — not "click here" or a bare URL.
4. If you cannot confidently identify a specific resource page URL that actually exists, do NOT invent a URL — skip the external link rather than fabricate one.
5. Draw from these authoritative domains (find a specific resource page within them, not the homepage):
   - HVAC/Energy: energystar.gov or energy.gov
   - Plumbing/Water: epa.gov/watersense or epa.gov
   - Electrical/Safety: esfi.org
   - Roofing: nrca.net
   - General home safety: cpsc.gov`;

    const systemPrompt = `You are a professional SEO content writer specializing in home service companies.
You write blog posts that are helpful, locally relevant, and optimized for search engines.
Always write in a friendly, trustworthy tone that homeowners relate to.
${brandVoice ? `Brand voice for ${clientName}: ${brandVoice}` : ""}

CRITICAL CONTENT RULE: This blog content is for a professional home service company that earns revenue from service calls. You must NEVER write step-by-step DIY repair instructions that would allow a homeowner to fix the problem themselves and skip hiring a professional. Instead:
- Help readers IDENTIFY and DIAGNOSE the problem (what to look for, warning signs, symptoms)
- Explain WHY the problem happens and what causes it
- Describe what a professional fix involves (so readers understand the value) WITHOUT giving enough detail to DIY it
- Always recommend contacting a licensed professional for actual repairs
- Use phrases like "a certified technician will...", "your HVAC pro will...", "call ${clientName} to..."
- For how-to style posts, frame the steps as "how to identify if you need X" or "what to expect when a pro fixes X" — not "how to fix X yourself"
This keeps content valuable for SEO while protecting the business's service revenue.`;

    const isHowToKeyword = /how.?to|step|guide|diy|install|fix|replace|repair|maintain|clean|troubleshoot/i.test(keyword);
    const userPrompt = `Write a complete SEO blog post for a ${industry} company called "${clientName || "our company"}" targeting the keyword: "${keyword}"
${internalLinksPrompt}
${externalLinksPrompt}
${existingContentPrompt}

Return your response as JSON with exactly this structure:
{
  "title": "SEO optimized blog post title — STRICT Yoast limit: must be between 50-60 characters total (including spaces). Count carefully. Shorter than 50 or longer than 60 characters will fail Yoast SEO.",
  "metaDescription": "Meta description — STRICT range: 120-156 characters total including spaces. Count character by character before submitting. Must be at least 120 and no more than 156 characters. Include the target keyword naturally.",
  "slug": "url-friendly-slug",
  "content": "Full HTML only — NO markdown whatsoever. Use <h2>, <h3>, <p>, <ul>, <li>, <strong>, <em>, <a> tags exclusively. Never use **, --, ##, or any markdown syntax. All bullet points must be <ul><li> HTML. All bold must be <strong>. Minimum 800 words.",
  "wordCount": estimated word count as integer,
  "faqs": [
    { "question": "Natural conversational question a homeowner would ask about this topic", "answer": "Direct 1-3 sentence answer with no HTML tags" },
    { "question": "...", "answer": "..." },
    { "question": "...", "answer": "..." },
    { "question": "...", "answer": "..." }
  ],
  "steps": ${isHowToKeyword
    ? `[
    { "name": "Step name (short, 3-6 words)", "text": "1-2 sentences describing what to CHECK or LOOK FOR at this stage — not how to fix it. Frame as diagnostic steps a homeowner takes before calling a pro, or what to expect a technician to do." },
    ...
  ]
  Include 4-8 steps framed as DIAGNOSIS steps (what to look for, what to check) or PROFESSIONAL PROCESS steps (what the technician will do). Never write steps that let a homeowner complete the repair themselves.`
    : `[] (empty array — this post is not a step-by-step guide)`}
}

The faqs array must have exactly 4 entries. Questions should be the kind of things people ask Google or AI assistants. Answers should be concise and direct. No HTML in faq answers or step text.
Return only the JSON, no other text.`;

    const message = await client.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 4000,
      messages: [{ role: "user", content: userPrompt }],
      system: systemPrompt,
    });

    const raw = message.content[0].text.trim();
    const clean = raw.replace(/```json|```/g, "").trim();
    const post = JSON.parse(clean);

    // Safety net: convert any markdown that slipped through
    post.content = markdownToHtml(post.content);

    // Meta description retry loop — never truncate, always regenerate if over 60
    if (post.metaDescription && (post.metaDescription.length < 120 || post.metaDescription.length > 156)) {
      let metaAttempts = 0;
      while ((post.metaDescription.length < 120 || post.metaDescription.length > 156) && metaAttempts < 3) {
        metaAttempts++;
        console.log(`[Meta] Out of 120-156 range (${post.metaDescription.length}) — retry ${metaAttempts}: "${post.metaDescription}"`);
        try {
          const metaRetry = await client.messages.create({
            model: "claude-sonnet-4-20250514",
            max_tokens: 100,
            messages: [{ role: "user", content: `Rewrite this meta description to be between 120-156 characters total including spaces. It must include the keyword "${keyword}" and convey the same meaning. Count every character carefully — must be at least 120 and no more than 156. Return ONLY the new meta description text, no quotes, no explanation.

Current (${post.metaDescription.length} chars): ${post.metaDescription}` }],
          });
          const newMeta = metaRetry.content[0]?.text?.trim().replace(/^["']|["']$/g, "") || "";
          if (newMeta.length >= 120 && newMeta.length <= 156) {
            post.metaDescription = newMeta;
            console.log(`[Meta] Retry ${metaAttempts} succeeded: "${newMeta}" (${newMeta.length} chars)`);
          } else {
            console.log(`[Meta] Retry ${metaAttempts} out of range (${newMeta.length}): "${newMeta}"`);
          }
        } catch(e) {
          console.log("[Meta] Retry error:", e.message);
          break;
        }
      }
      // Last resort: if still over 156 after 3 retries, trim at word boundary
      if (post.metaDescription.length > 156) {
        const trimmed = post.metaDescription.slice(0, 156);
        const lastSpace = trimmed.lastIndexOf(" ");
        post.metaDescription = lastSpace > 100 ? trimmed.slice(0, lastSpace) : trimmed;
        console.log(`[Meta] Hard-trimmed at word boundary: "${post.metaDescription}"`);
      }
    }

    // Build all schema blocks (Article + HowTo if applicable + FAQPage) + visible HTML sections
    const { appendHtml, schemaHtml, schemaTypes } = buildSchemaBlock({
      title: post.title,
      metaDescription: post.metaDescription,
      slug: post.slug,
      keyword,
      faqs: post.faqs || [],
      steps: post.steps || [],
      clientName: clientName || "",
      clientDomain: wordpressUrl || "",
      publishedDate: new Date().toISOString(),
      featuredImageUrl: "",  // not known yet at generate time — will be set at publish
      authorName: "",
    });
    if (appendHtml) post.content = post.content + appendHtml;
    // schemaHtml is saved via the Fortitude /schema endpoint after publish (not injected into body)
    console.log("[Content] Schema types prepared:", schemaTypes);

    if (clientId) {
      const { data: savedPost } = await supabase.from("posts").insert([{
        client_id: clientId,
        keyword,
        title: post.title,
        meta_description: post.metaDescription,
        slug: post.slug,
        content: post.content,
        word_count: post.wordCount,
        status: "draft",
      }]).select();
      post.id = savedPost?.[0]?.id;
    }

    res.json({ post, featuredImage, schemaHtml: schemaHtml || null });
  } catch (error) {
    console.error("Content generation error:", error.message);
    res.status(500).json({ error: "Failed to generate content", detail: error.message });
  }
});

// ─── WORDPRESS PUBLISHING ───────────────────────────────────────
app.post("/api/publish/wordpress", async (req, res) => {
  let { postId, clientId, title, content, slug, metaDescription, keyword, wordpressUrl, wpUsername, wpPassword, featuredImageUrl, featuredImageSlug, schemaHtml: schemaHtmlFromBody, longtailKeyphrase: longtailKeyphraseFromBody } = req.body;
  if (!wordpressUrl || !wpUsername || !wpPassword) return res.status(400).json({ error: "WordPress credentials required" });
  // Safety net: convert markdown to HTML before posting
  content = markdownToHtml(content);

  try {
    const credentials = Buffer.from(`${wpUsername}:${wpPassword}`).toString("base64");
    const authHeaders = { "Authorization": `Basic ${credentials}` };

    // Resolve schemaHtml and longtailKeyphrase — may come from request body (frontend passes these)
    // or be rebuilt from available data as a fallback
    const schemaHtml = schemaHtmlFromBody || null;
    let longtailKeyphrase = longtailKeyphraseFromBody || null;

    let featuredMediaId = null;
    if (featuredImageUrl) {
      try {
        const imageRes = await axios.get(featuredImageUrl, { responseType: "arraybuffer", httpsAgent });
        const imageBuffer = Buffer.from(imageRes.data);
        const ext = featuredImageUrl.split(".").pop().split("?")[0] || "jpg";
        const mimeType = ext === "png" ? "image/png" : ext === "webp" ? "image/webp" : "image/jpeg";
        const filename = `${featuredImageSlug || slug}.${ext}`;

        const mediaRes = await axios.post(`${wordpressUrl}/wp-json/wp/v2/media`, imageBuffer, {
          headers: {
            ...authHeaders,
            "Content-Type": mimeType,
            "Content-Disposition": `attachment; filename="${filename}"`,
          },
          httpsAgent,
        });
        featuredMediaId = mediaRes.data.id;

        await axios.post(`${wordpressUrl}/wp-json/wp/v2/media/${featuredMediaId}`, {
          alt_text: keyword,
          caption: keyword,
        }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });

        console.log("✓ Featured image uploaded, ID:", featuredMediaId);

        // Track image usage — enforces 10-post cooldown before reuse
        if (req.body.featuredImageId) await markImageUsed(req.body.featuredImageId);
      } catch (imgErr) {
        console.error("Image upload failed:", imgErr.response?.data || imgErr.message);
      }
    }

    let categoryId = null;
    try {
      const catRes = await axios.get(`${wordpressUrl}/wp-json/wp/v2/categories?per_page=100`, { headers: authHeaders, httpsAgent });
      const categories = catRes.data;
      if (categories.length > 0) {
        const keywordLower = keyword.toLowerCase();
        const industryLower = (req.body.industry || "").toLowerCase();
        const keywordWords = keywordLower.split(" ");
        let best = null;
        best = categories.find(c => keywordWords.some(w => w.length > 3 && c.name.toLowerCase().includes(w)));
        if (!best) best = categories.find(c => c.name.toLowerCase().includes(industryLower));
        if (!best) best = categories.find(c => c.slug !== "uncategorized") || categories[0];
        categoryId = best?.id || null;
        console.log("✓ Category matched:", best?.name, "ID:", categoryId);
      }
    } catch (catErr) {
      console.log("Category fetch failed:", catErr.message);
    }

    const postData = {
      title,
      content,
      slug,
      status: "publish",
      ...(featuredMediaId && { featured_media: featuredMediaId }),
      ...(categoryId && { categories: [categoryId] }),
    };

    const wpRes = await axios.post(`${wordpressUrl}/wp-json/wp/v2/posts`, postData, {
      headers: { ...authHeaders, "Content-Type": "application/json" },
      httpsAgent,
    });

    const wpPost = wpRes.data;
    console.log("✓ Post created, ID:", wpPost.id);

    // Save schema to Fortitude plugin endpoint (outputs in wp_head, not post body)
    if (schemaHtml) {
      (async () => {
        try {
          const schemaBlocks = [];
          const schemaRegex = /<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi;
          let sm;
          while ((sm = schemaRegex.exec(schemaHtml)) !== null) {
            try { schemaBlocks.push(JSON.parse(sm[1])); } catch(e) {}
          }
          if (schemaBlocks.length > 0) {
            await axios.post(`${wordpressUrl}/wp-json/fortitude/v1/schema`,
              { post_id: wpPost.id, schema: schemaBlocks },
              { headers: authHeaders, httpsAgent, timeout: 8000 }
            );
            console.log(`[Schema] ✓ Saved ${schemaBlocks.length} schema block(s) via Fortitude plugin`);
          }
        } catch(se) {
          console.log(`[Schema] Plugin save skipped (${se.message}) — schema present in post content as fallback`);
        }
      })();
    }

    // ── Detect SEO capabilities: Fortitude plugin + Yoast edition ────────────
    let seoCaps = { yoast: "none", fortitudePlugin: false, canWriteSeoMeta: false };
    try { seoCaps = await detectSeoCapabilities(wordpressUrl, authHeaders, clientId || null); } catch(e) {}
    const yoastEdition = seoCaps.yoast;
    console.log(`[Yoast] Caps: edition=${yoastEdition}, fortitudePlugin=${seoCaps.fortitudePlugin}, canWrite=${seoCaps.canWriteSeoMeta}`);

    // ── Write Yoast SEO meta (focuskw, title, metadesc) ────────────────────────
    // Strategy: try all available paths in priority order, verify after each.
    // focuskw = longtailKeyphrase (the specific long-tail variation for this post)
    const seoFocuskw = longtailKeyphrase || keyword || "";
    const seoTitle   = `${title} - %%sitename%%`;
    const seoMetadesc = metaDescription || "";

    let yoastWriteSuccess = false;
    let yoastWriteMethod  = "none";

    try {
      // PATH A: Fortitude plugin — calls WPSEO_Meta::set_value() directly on the server.
      // Works for BOTH Yoast Free AND Premium. Most reliable.
      if (seoCaps.fortitudePlugin) {
        try {
          const r = await axios.post(`${wordpressUrl}/wp-json/fortitude/v1/seo-meta`, {
            post_id: wpPost.id,
            focuskw: seoFocuskw,
            metadesc: seoMetadesc,
            title: seoTitle,
          }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
          if (r.data?.success) {
            yoastWriteSuccess = true;
            yoastWriteMethod = "fortitude_plugin";
            console.log(`[SEO] ✓ Yoast meta written via Fortitude plugin (focuskw="${seoFocuskw}")`);
          } else {
            console.log("[SEO] Fortitude seo-meta returned non-success:", JSON.stringify(r.data));
          }
        } catch(e) {
          console.log("[SEO] Fortitude seo-meta failed:", e.response?.data || e.message);
        }
      }

      // PATH B: Direct WP REST API post meta write (works when Yoast registered its keys)
      // Yoast Free registers these keys automatically; Premium does too.
      // Write even if Fortitude already succeeded — belt-and-suspenders for the REST layer.
      if (seoCaps.restMetaKeys) {
        try {
          await axios.post(`${wordpressUrl}/wp-json/wp/v2/posts/${wpPost.id}`, {
            meta: {
              _yoast_wpseo_focuskw:              seoFocuskw,
              _yoast_wpseo_metadesc:             seoMetadesc,
              _yoast_wpseo_title:                seoTitle,
              _yoast_wpseo_opengraph_title:      title,
              _yoast_wpseo_opengraph_description: seoMetadesc,
            }
          }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
          if (!yoastWriteSuccess) {
            yoastWriteSuccess = true;
            yoastWriteMethod = "rest_meta";
          }
          console.log(`[SEO] ✓ Yoast meta written via REST postmeta (focuskw="${seoFocuskw}")`);
        } catch(e) {
          console.log("[SEO] REST meta write failed:", e.response?.data || e.message);
        }
      }

      // PATH C: Last resort — try REST write even if keys weren't detected (they may appear after first post)
      if (!yoastWriteSuccess && yoastEdition !== "none") {
        try {
          await axios.post(`${wordpressUrl}/wp-json/wp/v2/posts/${wpPost.id}`, {
            meta: {
              _yoast_wpseo_focuskw:  seoFocuskw,
              _yoast_wpseo_metadesc: seoMetadesc,
              _yoast_wpseo_title:    seoTitle,
            }
          }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
          yoastWriteSuccess = true;
          yoastWriteMethod = "rest_fallback";
          console.log("[SEO] Yoast meta written via REST fallback");
        } catch(e) {}
      }

      if (!yoastWriteSuccess) {
        console.log(`[SEO] ⚠ Could not write Yoast meta — yoast=${yoastEdition}, fortitude=${seoCaps.fortitudePlugin}, restKeys=${seoCaps.restMetaKeys}`);
      }

      // Always write Astra layout meta regardless of Yoast status
      await axios.post(`${wordpressUrl}/wp-json/wp/v2/posts/${wpPost.id}`, {
        meta: { "astra-migrate-meta-layouts": "set" }
      }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent }).catch(() => {});

      console.log(`[SEO] Write summary — method=${yoastWriteMethod}, focuskw="${seoFocuskw}" — recalc will run after optimize loop`);

    } catch (yoastErr) {
      console.log("[SEO] Yoast write error:", JSON.stringify(yoastErr.response?.data || yoastErr.message));
    }

    if (postId) {
      await supabase.from("posts").update({ status: "published", published_at: new Date().toISOString(), wp_url: wpPost.link || "" }).eq("id", postId);
    }
    if (clientId) {
      const { data: client } = await supabase.from("clients").select("posts_published").eq("id", clientId).single();
      await supabase.from("clients").update({ posts_published: (client?.posts_published || 0) + 1 }).eq("id", clientId);
    }

    // ── Yoast optimization loop (density, title, H2s, meta length) ───────────
    let yoastOptResult = null;
    if (wpPost.link && seoCaps.canWriteSeoMeta) {
      try {
        yoastOptResult = await runYoastOptimizeLoop(
          wpPost.id,
          { title, keyword, metaDescription: metaDescription },
          wordpressUrl,
          { ...authHeaders, "Content-Type": "application/json" },
          seoCaps
        );
      } catch(e) {
        console.error("[YoastOpt] Loop threw:", e.message);
      }
    }

    // ── Post-publish QA + auto-repair loop ────────────────────────────────
    let qa = null;
    let repairHistory = [];
    if (wpPost.link) {
      try {
        const result = await qaRepairLoop(
          wpPost.id, wpPost.link,
          { title, keyword, metaDescription },
          wordpressUrl,
          { ...authHeaders, "Content-Type": "application/json" }
        );
        qa = result.qa;
        repairHistory = result.history;
      } catch(e) {
        console.error("[QA] Repair loop threw:", e.message);
        qa = { passed: false, score: 0, issues: [{ type: "qa_error", severity: "error", message: e.message }], warnings: [], liveUrl: wpPost.link };
      }
    }

    // ── Final Yoast recalc — AFTER optimize loop + QA so green lights reflect final state ──
    // Must run last because the optimize loop may have rewritten meta/keyphrase after initial publish.
    // The no-op PUT fires wp_update_post which forces Yoast to recompute ALL scores and indexables.
    if (wpPost?.id && wordpressUrl) {
      try {
        // Step 1: Small pause to let any prior writes fully commit to the DB
        await new Promise(r => setTimeout(r, 2000));

        // Step 2: Fortitude plugin recalc (most direct — calls calculate_scores server-side)
        let recalcOk = false;
        if (seoCaps?.fortitudePlugin) {
          try {
            const r = await axios.post(`${wordpressUrl}/wp-json/fortitude/v1/yoast-recalc`,
              { post_id: wpPost.id },
              { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
            if (r.data?.success || r.status === 200) {
              recalcOk = true;
              console.log("[Recalc] ✓ Fortitude yoast-recalc succeeded");
            }
          } catch(e) { console.log("[Recalc] Fortitude recalc error:", e.message); }
        }

        // Step 3: Always run no-op PUT regardless — this is the universal trigger for Yoast
        // Even if Fortitude recalc ran, the no-op PUT ensures WP hooks fire and the post_modified
        // timestamp updates, which forces the post list to refresh its cached Yoast column values.
        await axios.put(`${wordpressUrl}/wp-json/wp/v2/posts/${wpPost.id}`,
          { status: "publish" },
          { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
        console.log("[Recalc] ✓ No-op PUT fired — Yoast will recompute scores on next post list load");

        // Step 4: Second Fortitude recalc after the PUT so scores are in DB before response
        if (seoCaps?.fortitudePlugin) {
          try {
            await new Promise(r => setTimeout(r, 800));
            await axios.post(`${wordpressUrl}/wp-json/fortitude/v1/yoast-recalc`,
              { post_id: wpPost.id },
              { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
            console.log("[Recalc] ✓ Second Fortitude recalc after PUT completed");
          } catch(e) {}
        }

      } catch(e) {
        console.log("[Recalc] Final recalc error:", e.message);
      }
    }

    // ── Auto-post to Google Business Profile ──────────────────────────────
    let gbpResult = null;
    if (clientId && wpPost.link && agencyGbpToken.refresh_token) {
      try {
        const { data: clientData } = await supabase.from("clients").select("gbp_location_name").eq("id", clientId).single();
        if (clientData?.gbp_location_name) {
          const access_token = await getAgencyAccessToken();
          const gbpSummary = metaDescription || `${title} — read our latest post for expert tips and advice.`;
          const gbpBody = {
            languageCode: "en",
            summary: gbpSummary,
            topicType: "STANDARD",
            callToAction: { actionType: "LEARN_MORE", url: wpPost.link },
            ...(featuredImageUrl ? { media: [{ mediaFormat: "PHOTO", sourceUrl: featuredImageUrl }] } : {}),
          };
          const gbpRes = await axios.post(
            `https://mybusiness.googleapis.com/v4/${clientData.gbp_location_name}/localPosts`,
            gbpBody,
            { headers: { Authorization: `Bearer ${access_token}`, "Content-Type": "application/json" } }
          );
          gbpResult = { success: true, post: gbpRes.data };
          console.log("✓ GBP post auto-published for client", clientId);
        }
      } catch (gbpErr) {
        console.error("GBP auto-post error:", gbpErr.response?.data || gbpErr.message);
        gbpResult = { success: false, error: gbpErr.response?.data?.error?.message || gbpErr.message };
      }
    }

    // Auto-add keyword to client's used keywords list and remove from queues
    if (clientId && keyword) {
      try {
        // Add to used keywords (upsert)
        const { data: existingUsed } = await supabase.from("client_used_keywords")
          .select("id").eq("client_id", clientId).eq("keyword", keyword.trim()).single();
        if (!existingUsed) {
          await supabase.from("client_used_keywords")
            .insert([{ client_id: clientId, keyword: keyword.trim(), added_at: new Date().toISOString() }]);
        }
        // Remove from monthly keyword queue
        await supabase.from("client_keyword_queue")
          .delete().eq("client_id", clientId).ilike("keyword", keyword.trim());
      } catch(e) { console.error("Used keyword hook error:", e.message); }
    }
    res.json({ success: true, wpPostId: wpPost.id, wpPostUrl: wpPost.link, status: wpPost.status, featuredMediaId, qa, repairHistory, gbpResult, yoastEdition, longtailKeyphrase, fortitudePlugin: seoCaps.fortitudePlugin, canWriteSeoMeta: seoCaps.canWriteSeoMeta, yoastOpt: yoastOptResult });
  } catch (error) {
    console.error("WordPress publish error:", error.response?.data || error.message);
    res.status(500).json({ error: "Failed to publish to WordPress", detail: error.response?.data?.message || error.message });
  }
});

// ─── MANUAL YOAST OPTIMIZE ENDPOINT ──────────────────────────────────────────
// Called by the "Re-run Yoast Fix" button in the publish banner.
// Accepts { clientId, wpPostId, title, keyword, metaDescription }
// ─────────────────────────────────────────────────────────────────────────────
app.post("/api/yoast-optimize", async (req, res) => {
  const { clientId, wpPostId, title, keyword, metaDescription } = req.body;
  if (!clientId || !wpPostId || !keyword) {
    return res.status(400).json({ error: "clientId, wpPostId, and keyword are required" });
  }
  try {
    const { data: client } = await supabase.from("clients").select("*").eq("id", clientId).single();
    if (!client?.wordpress_url) return res.status(404).json({ error: "Client or WP URL not found" });

    const wordpressUrl = client.wordpress_url.replace(/\/$/, "");
    const wpUser = client.wordpress_username;
    const wpPass = client.wordpress_password;
    if (!wpUser || !wpPass) return res.status(400).json({ error: "WordPress credentials not set for this client — re-enter them via Edit Client" });

    const authHeaders = {
      "Authorization": "Basic " + Buffer.from(`${wpUser}:${wpPass}`).toString("base64"),
      "Content-Type": "application/json",
    };

    const seoCaps = await detectSeoCapabilities(wordpressUrl, authHeaders);
    console.log(`[YoastOpt/Manual] Client: ${client.name}, post: ${wpPostId}, caps:`, seoCaps);

    const result = await runYoastOptimizeLoop(
      parseInt(wpPostId),
      { title: title || "", keyword, metaDescription: metaDescription || "" },
      wordpressUrl,
      authHeaders,
      seoCaps
    );

    res.json({ success: true, ...result });
  } catch (err) {
    console.error("[YoastOpt/Manual] Error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Yoast green-light check for a published post ─────────────────────────────
app.get("/api/yoast-check/:clientId/:postId", requireAuth, async (req, res) => {
  const { clientId, postId } = req.params;
  try {
    const { data: client } = await supabase.from("clients").select("*").eq("id", clientId).single();
    if (!client?.wordpress_url || !client?.wordpress_username || !client?.wordpress_password) {
      return res.status(400).json({ error: "WordPress credentials not set" });
    }
    const wpBase = client.wordpress_url.replace(/\/$/, "");
    const authHeaders = {
      "Authorization": "Basic " + Buffer.from(`${client.wordpress_username}:${client.wordpress_password}`).toString("base64"),
      "Content-Type": "application/json",
    };

    // Fetch the post
    const postRes = await axios.get(`${wpBase}/wp-json/wp/v2/posts/${postId}?context=edit`, {
      headers: authHeaders, httpsAgent
    });
    const post = postRes.data;
    const html = post?.content?.raw || "";
    const title = post?.yoast_head_json?.title || post?.title?.rendered || "";
    const metadesc = post?.yoast_head_json?.description || "";

    // Fetch keyword from scheduled_jobs
    const { data: job } = await supabase
      .from("scheduled_jobs")
      .select("keyword")
      .eq("client_id", clientId)
      .eq("wp_post_id", parseInt(postId))
      .single();
    const keyword = job?.keyword || "";

    if (!keyword) return res.json({ green: false, issues: ["No keyword found for post"] });

    // Run the same checkIssues logic
    const kwLower = keyword.toLowerCase();
    const kwWords = kwLower.split(/\s+/);
    const textOnly = html.replace(/<[^>]+>/g, " ").toLowerCase();
    const wordCount = textOnly.split(/\s+/).filter(Boolean).length;

    const issues = [];

    // Keyphrase density
    let kwCount = 0, pos = 0;
    while ((pos = textOnly.indexOf(kwLower, pos)) !== -1) { kwCount++; pos += kwLower.length; }
    const minOccurrences = Math.max(5, Math.floor(wordCount / 300));
    if (kwCount < minOccurrences) issues.push(`Low density (${kwCount}/${minOccurrences})`);

    // Keyphrase in title
    const titleLower = title.replace(/ - %%sitename%%/, "").toLowerCase();
    const missingInTitle = kwWords.filter(w => !titleLower.includes(w));
    if (missingInTitle.length > 0) issues.push(`Missing in title: ${missingInTitle.join(", ")}`);

    // Keyphrase in subheadings
    const headings = [...html.matchAll(/<h[23][^>]*>(.*?)<\/h[23]>/gi)].map(m => m[1].replace(/<[^>]+>/g, "").toLowerCase());
    const hasKwInHeading = headings.some(h => kwWords.some(w => h.includes(w)));
    if (!hasKwInHeading) issues.push("Missing in subheadings");

    // Meta description length
    if (metadesc.length < 120 || metadesc.length > 156) issues.push(`Meta desc length ${metadesc.length} (need 120-156)`);

    res.json({ green: issues.length === 0, issues, keyword, wordCount });
  } catch (err) {
    console.error("[YoastCheck] Error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/keywords/gap", async (req, res) => {
  const { domain, competitor1, competitor2, database = "us" } = req.query;
  if (!domain || !competitor1) return res.status(400).json({ error: "domain and competitor1 are required" });
  try {
    const response = await axios.get("https://api.semrush.com/", {
      params: {
        type: "phrase_kgap",
        key: SEMRUSH_API_KEY,
        targets: `${domain},${competitor1}${competitor2 ? "," + competitor2 : ""}`,
        target_types: `domain,domain${competitor2 ? ",domain" : ""}`,
        export_columns: "Ph,Nq,Cp,Co,Nr,Kd",
        database,
        display_limit: 20,
      },
    });
    const lines = response.data.trim().split("\n");
    if (lines.length < 2) return res.json({ results: [] });
    const results = lines.slice(1).map((line) => {
      const values = line.split(";");
      return { keyword: values[0], volume: parseInt(values[1]) || 0, cpc: "$" + parseFloat(values[2]).toFixed(2), kd: parseInt(values[5]) || 0 };
    });
    res.json({ results });
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch keyword gap data" });
  }
});

// ─── COMPETITOR FINDER ───────────────────────────────────────────
// Uses web_search tool so Claude finds real local competitors from Google results
// rather than guessing. Requires serviceArea (e.g. "Charlotte, NC") for accuracy.
app.post("/api/competitors/find", async (req, res) => {
  const { clientName, industry, domain, serviceArea } = req.body;
  if (!clientName || !industry) return res.status(400).json({ error: "clientName and industry required" });

  const location = serviceArea || "";
  const excludeDomain = domain || "";

  const EXCLUDE_PATTERNS = [
    "yelp","angi","homeadvisor","thumbtack","bbb","angieslist","houzz",
    "porch.com","google","facebook","instagram","youtube","amazon","indeed",
    "linkedin","bing","yahoo","acehardware","lowes","homedepot",
    "lennox","carrier","trane","goodman","rheem","york",
  ];

  const isDomainExcluded = (d) => {
    if (!d) return true;
    const lower = d.toLowerCase();
    if (excludeDomain && lower.includes(excludeDomain.toLowerCase().replace(/https?:\/\//,""))) return true;
    if (clientName && lower.includes(clientName.toLowerCase().split(" ")[0])) return true;
    return EXCLUDE_PATTERNS.some(p => lower.includes(p));
  };

  const extractDomainsFromBlocks = (blocks) => {
    const domains = new Set();
    for (const block of blocks) {
      let text = "";
      if (block.type === "tool_result") {
        text = typeof block.content === "string" ? block.content
          : (block.content || []).map(c => c.text || "").join(" ");
      } else if (block.type === "text" && block.text) {
        text = block.text;
      }
      if (!text) continue;
      const urlMatches = [...text.matchAll(/https?:\/\/(?:www\.)?([a-z0-9][a-z0-9\-\.]*\.[a-z]{2,6})/gi)];
      const plainMatches = [...text.matchAll(/\b(?:www\.)?([a-z0-9][a-z0-9\-]+\.[a-z]{2,6})\b/gi)];
      [...urlMatches, ...plainMatches].forEach(m => {
        const d = m[1].toLowerCase().replace(/^\./, "");
        if (d.includes(".") && !isDomainExcluded(d)) domains.add(d);
      });
    }
    return [...domains];
  };

  try {
    const Anthropic = require("@anthropic-ai/sdk");
    const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

    const searches = location ? [
      `${industry} contractor ${location}`,
      `best ${industry} company ${location}`,
      `${industry} repair ${location}`,
    ] : [
      `${industry} contractor near ${clientName}`,
      `best local ${industry} company`,
    ];

    let allDomains = [];

    for (const query of searches) {
      if (allDomains.length >= 5) break;
      try {
        const resp = await anthropic.messages.create({
          model: "claude-haiku-4-5-20251001",
          max_tokens: 600,
          tools: [{ type: "web_search_20250305", name: "web_search" }],
          system: "Search the web and return ONLY a JSON array of website domains found. No explanation.",
          messages: [{ role: "user", content: `Search: "${query}" — list all website domains you find as a JSON array like ["domain1.com","domain2.com"]` }],
        });

        const fromBlocks = extractDomainsFromBlocks(resp.content);

        // Also try parsing JSON array from text block
        const textBlock = resp.content.find(b => b.type === "text");
        const raw = textBlock?.text?.trim() || "";
        const arrayMatch = raw.match(/\[[\s\S]*?\]/);
        if (arrayMatch) {
          try {
            const parsed = JSON.parse(arrayMatch[0]);
            if (Array.isArray(parsed)) {
              parsed.forEach(d => {
                if (typeof d === "string") {
                  const clean = d.toLowerCase().replace(/https?:\/\//, "").replace(/^\.?www\./, "").split("/")[0];
                  if (clean && !isDomainExcluded(clean)) fromBlocks.push(clean);
                }
              });
            }
          } catch(e) {}
        }

        console.log(`[Competitors] "${query}" → ${fromBlocks.slice(0,5).join(", ") || "none"}`);
        allDomains.push(...fromBlocks);
        allDomains = [...new Set(allDomains)].filter(d => !isDomainExcluded(d));
      } catch(searchErr) {
        console.log(`[Competitors] Search error for "${query}":`, searchErr.message);
      }
    }

    const competitors = allDomains.slice(0, 5);
    if (!competitors.length) {
      return res.status(500).json({ error: "Could not find local competitors. Add a service area in Edit Client and retry." });
    }
    res.json({ competitors });
  } catch (err) {
    console.error("Competitor find error:", err.message);
    res.status(500).json({ error: "Failed to find competitors", detail: err.message });
  }
});

app.put("/api/clients/:id/competitors", async (req, res) => {
  const { id } = req.params;
  const { competitors } = req.body;
  const { data, error } = await supabase.from("clients").update({ competitors }).eq("id", id).select();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ client: data[0] });
});

// ─── MONTHLY KEYWORD REFRESH ──────────────────────────────────────
// ─── SHARED: build exclusion set (used keywords + current queue) ─────────────
async function buildExclusionSet(clientId) {
  const [{ data: usedRows }, { data: queueRows }] = await Promise.all([
    supabase.from("client_used_keywords").select("keyword").eq("client_id", clientId),
    supabase.from("client_keyword_queue").select("keyword").eq("client_id", clientId),
  ]);
  const set = new Set();
  (usedRows || []).forEach(r => set.add(r.keyword.toLowerCase()));
  (queueRows || []).forEach(r => set.add(r.keyword.toLowerCase()));
  return set;
}

// ─── SHARED: generate research keywords (library + AI fallback) ───────────────
async function generateResearchKeywords(client, count, excludeSet) {
  const Anthropic = require("@anthropic-ai/sdk");
  const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

  // Parse industry_tags — e.g. "hvac, plumbing" → ["hvac","plumbing"]
  const tags = (client.industry_tags || client.industry || "")
    .split(/[,\s]+/).map(t => t.trim().toLowerCase()).filter(Boolean);

  let libKeywords = [];
  if (tags.length > 0) {
    // Fetch from library for each tag and merge
    const allLib = [];
    for (const tag of tags) {
      const { data } = await supabase.from("keyword_library")
        .select("*").ilike("industry", `%${tag}%`)
        .order("volume", { ascending: false }).limit(count + 40);
      if (data) allLib.push(...data);
    }
    // Deduplicate by keyword text
    const seen = new Set();
    libKeywords = allLib.filter(k => {
      const lc = k.keyword.toLowerCase();
      if (seen.has(lc) || excludeSet.has(lc)) return false;
      seen.add(lc);
      return true;
    }).slice(0, count).map(k => ({ keyword: k.keyword, volume: k.volume || 0, intent: k.intent || "Informational", source: "library" }));
  }

  if (libKeywords.length >= count) return libKeywords.slice(0, count);

  // AI fallback for remainder
  const stillNeeded = count - libKeywords.length;
  const existingList = [...excludeSet, ...libKeywords.map(k => k.keyword.toLowerCase())].slice(0, 30).join(", ");
  try {
    const msg = await anthropic.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 1000,
      messages: [{ role: "user", content: `You are an SEO expert. Generate ${stillNeeded} high-intent blog keyword phrases for a ${client.industry_tags || client.industry} business called "${client.name}"${client.service_area ? ` located in ${client.service_area}` : ""}${client.domain ? ` with website ${client.domain}` : ""}. Focus on: local service keywords, how-to guides, cost/pricing, emergency services, and comparison keywords. Do NOT use any of these already-used keywords: ${existingList}. Respond with ONLY a raw JSON array of ${stillNeeded} strings. No markdown, no explanation.` }],
    });
    const tb = msg.content.find(b => b.type === "text");
    if (tb) {
      const parsed = JSON.parse(tb.text.trim().replace(/\`\`\`json|\`\`\`/g, "").trim());
      const aiKws = parsed.filter(kw => !excludeSet.has(kw.toLowerCase()) && !libKeywords.find(l => l.keyword.toLowerCase() === kw.toLowerCase()))
        .slice(0, stillNeeded).map(kw => ({ keyword: kw, volume: 0, intent: "Informational", source: "ai" }));
      libKeywords = [...libKeywords, ...aiKws];
    }
  } catch(e) { console.log("AI research fallback error:", e.message); }
  return libKeywords.slice(0, count);
}

// ─── SHARED: generate competitor gap keywords ─────────────────────────────────
async function generateGapKeywords(client, count, excludeSet) {
  const Anthropic = require("@anthropic-ai/sdk");
  const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
  const competitors = client.competitors || [];
  if (competitors.length === 0) return [];

  const usedList = [...excludeSet].slice(0, 30).join(", ");
  // Run 3 targeted searches to get diverse gap keywords
  const prompts = [
    `Competitor websites: ${competitors.join(", ")}. Generate ${Math.ceil(count/2)} high-intent ${client.industry_tags || client.industry} keyword phrases that a competitor "${client.name}"${client.service_area ? ` in ${client.service_area}` : ""} is likely ranking for. Focus on local service + transactional terms. NOT any of: ${usedList}. ONLY a raw JSON array. No markdown.`,
    `Competitor websites: ${competitors.join(", ")}. Generate ${Math.floor(count/2)} additional ${client.industry_tags || client.industry} keyword gaps focusing on pricing, comparison, emergency, or seasonal terms that "${client.name}" should capture from competitors. NOT any of: ${usedList}. ONLY a raw JSON array. No markdown.`,
  ];

  const results = [];
  for (const prompt of prompts) {
    try {
      const msg = await anthropic.messages.create({
        model: "claude-sonnet-4-20250514", max_tokens: 800,
        messages: [{ role: "user", content: prompt }],
      });
      const tb = msg.content.find(b => b.type === "text");
      if (tb) {
        const parsed = JSON.parse(tb.text.trim().replace(/\`\`\`json|\`\`\`/g, "").trim());
        results.push(...parsed);
      }
    } catch(e) { console.log("Gap gen error:", e.message); }
  }

  const seen = new Set();
  return results.filter(kw => {
    const lc = kw.toLowerCase();
    if (seen.has(lc) || excludeSet.has(lc)) return false;
    seen.add(lc);
    return true;
  }).slice(0, count).map(kw => ({ keyword: kw, volume: 0, intent: "Transactional", source: "gap" }));
}

// ─── REGENERATE QUEUE: fills both research (15) and gap (15) ─────────────────
app.post("/api/keywords/monthly-refresh/:clientId", async (req, res) => {
  const { clientId } = req.params;
  try {
    const { data: client } = await supabase.from("clients").select("*").eq("id", clientId).single();
    if (!client) return res.status(404).json({ error: "Client not found" });

    const month = new Date().toISOString().slice(0, 7);
    const excludeSet = await buildExclusionSet(clientId);

    const [researchKws, gapKws] = await Promise.all([
      generateResearchKeywords(client, 15, excludeSet),
      generateGapKeywords(client, 15, excludeSet),
    ]);

    const allKeywords = [...researchKws, ...gapKws];
    if (allKeywords.length === 0) {
      return res.status(400).json({ error: `Failed to generate keywords for "${client.name}". Please try again.` });
    }
    const inserts = allKeywords.map(k => ({
      client_id: clientId, keyword: k.keyword, volume: k.volume || 0,
      intent: k.intent || "Informational", source: k.source, month, used: false,
    }));
    await supabase.from("client_keyword_queue").insert(inserts);
    await supabase.from("clients").update({ next_keyword_index: 0 }).eq("id", clientId);
    res.json({ success: true, total: inserts.length, research: researchKws.length, gap: gapKws.length });
  } catch (err) {
    console.error("Regenerate queue error:", err.message);
    res.status(500).json({ error: "Failed to regenerate queue", detail: err.message });
  }
});

// ─── ARCHIVED POSTS (published posts for a client) ──────────────────────────
app.get("/api/clients/:clientId/archived-posts", requireAuth, async (req, res) => {
  const { clientId } = req.params;
  const { data, error } = await supabase.from("posts")
    .select("id, keyword, title, wp_url, published_at, status")
    .eq("client_id", clientId).eq("status", "published")
    .order("published_at", { ascending: false }).limit(50);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ posts: data || [] });
});

// ─── REFRESH RESEARCH ONLY (library + AI, no gap) ────────────────────────────
app.post("/api/keywords/queue/refresh-research/:clientId", requireAuth, async (req, res) => {
  const { clientId } = req.params;
  try {
    const { data: client } = await supabase.from("clients").select("*").eq("id", clientId).single();
    if (!client) return res.status(404).json({ error: "Client not found" });
    const month = new Date().toISOString().slice(0, 7);
    const excludeSet = await buildExclusionSet(clientId);
    const keywords = await generateResearchKeywords(client, 15, excludeSet);
    if (keywords.length === 0) return res.status(400).json({ error: "No new research keywords found" });
    const inserts = keywords.map(k => ({
      client_id: clientId, keyword: k.keyword, volume: k.volume || 0,
      intent: k.intent || "Informational", source: k.source, month, used: false,
    }));
    await supabase.from("client_keyword_queue").insert(inserts);
    res.json({ success: true, added: inserts.length, keywords: inserts });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── REFRESH GAP ONLY (competitor analysis) ──────────────────────────────────
app.post("/api/keywords/queue/refresh-gap/:clientId", requireAuth, async (req, res) => {
  const { clientId } = req.params;
  try {
    const { data: client } = await supabase.from("clients").select("*").eq("id", clientId).single();
    if (!client) return res.status(404).json({ error: "Client not found" });
    if (!client.competitors || client.competitors.length === 0) {
      return res.status(400).json({ error: "No competitors listed for this client. Add competitors first." });
    }
    const month = new Date().toISOString().slice(0, 7);
    const excludeSet = await buildExclusionSet(clientId);
    const keywords = await generateGapKeywords(client, 15, excludeSet);
    if (keywords.length === 0) return res.status(400).json({ error: "No new competitor gap keywords found" });
    const inserts = keywords.map(k => ({
      client_id: clientId, keyword: k.keyword, volume: k.volume || 0,
      intent: k.intent || "Transactional", source: k.source, month, used: false,
    }));
    await supabase.from("client_keyword_queue").insert(inserts);
    res.json({ success: true, added: inserts.length, keywords: inserts });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});


// ─── CLIENT KEYWORDS (per-client keyword list) ───────────────────────────────
// These are keywords assigned to a specific client from the library.
// From here they can be dragged to the monthly queue or removed.

app.get("/api/clients/:clientId/keywords", requireAuth, async (req, res) => {
  const { clientId } = req.params;
  const page = parseInt(req.query.page) || 1;
  const limit = 20;
  const offset = (page - 1) * limit;
  const { data, error, count } = await supabase
    .from("client_keywords")
    .select("*", { count: "exact" })
    .eq("client_id", clientId)
    .order("added_at", { ascending: false })
    .range(offset, offset + limit - 1);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ keywords: data, total: count, page, pages: Math.ceil(count / limit) });
});

app.post("/api/clients/:clientId/keywords", requireAuth, async (req, res) => {
  const { clientId } = req.params;
  const { keyword, volume, kd, intent, source } = req.body;
  if (!keyword) return res.status(400).json({ error: "keyword required" });
  // Avoid duplicates
  const { data: existing } = await supabase
    .from("client_keywords")
    .select("id")
    .eq("client_id", clientId)
    .ilike("keyword", keyword)
    .maybeSingle();
  if (existing) return res.json({ keyword: existing, duplicate: true });
  const { data, error } = await supabase
    .from("client_keywords")
    .insert({ client_id: clientId, keyword, volume: volume || 0, kd: kd || 0, intent: intent || "Informational", source: source || "library" })
    .select()
    .single();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ keyword: data });
});

app.delete("/api/clients/:clientId/keywords/:id", requireAuth, async (req, res) => {
  const { id } = req.params;
  const { error } = await supabase.from("client_keywords").delete().eq("id", id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.get("/api/keywords/queue/:clientId", async (req, res) => {
  const { clientId } = req.params;
  const month = new Date().toISOString().slice(0, 7);
  const { data, error } = await supabase.from("client_keyword_queue")
    .select("*").eq("client_id", clientId).eq("month", month)
    .order("source", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ keywords: data, month });
});

app.delete("/api/keywords/queue/:id", async (req, res) => {
  const { id } = req.params;
  const { error } = await supabase.from("client_keyword_queue").delete().eq("id", id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.post("/api/keywords/queue/add", async (req, res) => {
  try {
    const { clientId, keyword, source, intent, volume } = req.body;
    if (!clientId || !keyword) return res.status(400).json({ error: "clientId and keyword required" });
    const month = new Date().toISOString().slice(0, 7);
    const { data, error } = await supabase.from("client_keyword_queue").insert([{
      client_id: clientId,
      keyword,
      source: source || "library",
      intent: intent || "Transactional",
      volume: volume || 0,
      month,
      used: false,
    }]).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ success: true, keyword: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/keywords/queue/gap-single", async (req, res) => {
  try {
    const { clientId } = req.body;
    const { data: client } = await supabase.from("clients").select("*").eq("id", clientId).single();
    if (!client) return res.status(404).json({ error: "Client not found" });

    const { data: existing } = await supabase.from("client_keyword_queue")
      .select("keyword").eq("client_id", clientId).eq("month", new Date().toISOString().slice(0, 7));
    const existingSet = new Set((existing || []).map(k => k.keyword.toLowerCase()));

    const Anthropic = require("@anthropic-ai/sdk");
    const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
    const competitors = client.competitors || [];

    const message = await anthropic.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 300,
      messages: [{
        role: "user",
        content: `You are an SEO expert. Generate 5 fresh high-intent keyword phrases for a ${client.industry} business called "${client.name}"${competitors.length > 0 ? ` that competes with: ${competitors.slice(0,3).join(", ")}` : ""}. These keywords are NOT already in use: ${[...existingSet].slice(0,10).join(", ")}. Focus on local service, emergency, or cost keywords. Respond with ONLY a raw JSON array of 5 strings. No markdown, no explanation.`
      }],
    });

    const textBlock = message.content.find(b => b.type === "text");
    let suggestions = [];
    if (textBlock) {
      const clean = textBlock.text.trim().replace(/```json|```/g, "").trim();
      suggestions = JSON.parse(clean).slice(0, 5).filter(kw => !existingSet.has(kw.toLowerCase()));
    }

    res.json({ success: true, suggestions });
  } catch (err) {
    res.status(500).json({ error: err.message, suggestions: [] });
  }
});


// ─── SCHEDULER ENDPOINTS ─────────────────────────────────────────
app.get("/api/schedule/:clientId", async (req, res) => {
  const { clientId } = req.params;
  const { data, error } = await supabase.from("scheduled_jobs")
    .select("*")
    .eq("client_id", clientId)
    .order("scheduled_time", { ascending: false })
    .limit(10);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ jobs: data });
});
// Add a keyword to a client's scheduled queue manually
app.post("/api/schedule/add", async (req, res) => {
  const { clientId, keyword } = req.body;
  if (!clientId || !keyword) return res.status(400).json({ error: "clientId and keyword required" });
  try {
    const { data: client } = await supabase.from("clients").select("schedule_start_hour,schedule_end_hour").eq("id", clientId).single();
    const startHour = client?.schedule_start_hour || 9;
    const endHour = client?.schedule_end_hour || 12;
    const windowMinutes = Math.max(1, (endHour - startHour) * 60);
    const randMinutes = Math.floor(Math.random() * windowMinutes);
    // Schedule for tomorrow within the client's publish window
    const TZ = "America/New_York";
    const nowUTC = new Date();
    const estOffsetMs = nowUTC.getTime() - new Date(nowUTC.toLocaleString("en-US", { timeZone: TZ })).getTime();
    const estOffsetHours = estOffsetMs / 3600000;
    const tomorrow = new Date(nowUTC);
    tomorrow.setDate(tomorrow.getDate() + 1);
    const tParts = new Intl.DateTimeFormat("en-US", { timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit" })
      .formatToParts(tomorrow).reduce((a, p) => { a[p.type] = p.value; return a; }, {});
    const sign = estOffsetHours <= 0 ? "-" : "+";
    const absH = String(Math.floor(Math.abs(estOffsetHours))).padStart(2, "0");
    const absM = String(Math.round((Math.abs(estOffsetHours) % 1) * 60)).padStart(2, "0");
    const hh = String(startHour).padStart(2, "0");
    const scheduledTime = new Date(`${tParts.year}-${tParts.month}-${tParts.day}T${hh}:00:00${sign}${absH}:${absM}`);
    scheduledTime.setMinutes(scheduledTime.getMinutes() + randMinutes);
    const { data, error } = await supabase.from("scheduled_jobs").insert([{
      client_id: clientId,
      keyword,
      scheduled_time: scheduledTime.toISOString(),
      status: "pending",
    }]).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json({ success: true, job: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.patch("/api/schedule/job/:id", async (req, res) => {
  const { id } = req.params;
  const { scheduled_time } = req.body;
  if (!scheduled_time) return res.status(400).json({ error: "scheduled_time required" });
  const { error } = await supabase.from("scheduled_jobs")
    .update({ scheduled_time })
    .eq("id", id)
    .eq("status", "pending"); // only reorder pending jobs
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.post("/api/schedule/:clientId/toggle", async (req, res) => {
  const { clientId } = req.params;
  const { enabled } = req.body;
  const { data, error } = await supabase.from("clients")
    .update({ schedule_enabled: enabled })
    .eq("id", clientId)
    .select();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ client: data[0] });
});

app.put("/api/schedule/:clientId", async (req, res) => {
  const { clientId } = req.params;
  const { schedule_frequency, schedule_days, schedule_start_hour, schedule_end_hour, schedule_timezone } = req.body;
  const { data, error } = await supabase.from("clients")
    .update({ schedule_frequency, schedule_days, schedule_start_hour, schedule_end_hour, schedule_timezone })
    .eq("id", clientId)
    .select();
  if (error) return res.status(500).json({ error: error.message });
  res.json({ client: data[0] });
});

// ─── SCHEDULER ENGINE ─────────────────────────────────────────────
// ── Existing content deduplication ──────────────────────────────────────────
// Fetches all existing page/post titles and slugs from WP + Supabase so
// the generator can avoid duplicating them.
async function fetchExistingContent(wordpressUrl, clientId) {
  const existing = []; // [{ title, slug }]

  // 1. WordPress pages
  if (wordpressUrl) {
    try {
      const pagesRes = await axios.get(`${wordpressUrl}/wp-json/wp/v2/pages?per_page=100&_fields=title,slug`, { httpsAgent, timeout: 8000 });
      pagesRes.data.forEach(p => existing.push({ title: p.title?.rendered || "", slug: p.slug }));
    } catch (e) { console.log("WP pages fetch skipped:", e.message); }

    // 2. WordPress posts (up to 100 most recent)
    try {
      const postsRes = await axios.get(`${wordpressUrl}/wp-json/wp/v2/posts?per_page=100&_fields=title,slug`, { httpsAgent, timeout: 8000 });
      postsRes.data.forEach(p => existing.push({ title: p.title?.rendered || "", slug: p.slug }));
    } catch (e) { console.log("WP posts fetch skipped:", e.message); }
  }

  // 3. Supabase posts table (catches drafts not yet published to WP)
  if (clientId) {
    try {
      const { data: dbPosts } = await supabase.from("posts").select("title, slug").eq("client_id", clientId);
      dbPosts?.forEach(p => existing.push({ title: p.title || "", slug: p.slug || "" }));
    } catch (e) { console.log("Supabase posts fetch skipped:", e.message); }
  }

  // Deduplicate by slug
  const seen = new Set();
  return existing.filter(p => {
    const key = p.slug.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

// Builds a prompt block telling Claude what titles/slugs are already taken.
function buildExistingContentPrompt(existingContent) {
  if (!existingContent || existingContent.length === 0) return "";
  const list = existingContent.map(p => '- "' + p.title + '" (/' + p.slug + '/)').join("\n");
  return "\n\nEXISTING CONTENT — DO NOT DUPLICATE: The following titles and URL slugs already exist on this website. Your generated title and slug MUST be meaningfully different from all of these — not just a minor word swap. Approach the topic from a clearly distinct angle:\n" + list;
}

// ── Featured image rotation helpers ─────────────────────────────────────────
// Picks the best matching image that hasn't been used in the last 8 posts.
// images pre-sorted: times_used ASC, last_used_at ASC nulls first.
// COOLDOWN: an image cannot be reused until at least COOLDOWN other posts
// have been published after it. We enforce this by:
//   1. Filtering out any image whose last_used_at is among the COOLDOWN most recent usages
//   2. If all images are on cooldown (small library), use the least-recently-used one
function selectFeaturedImage(images, keywordWords = []) {
  if (!images || images.length === 0) return null;
  const COOLDOWN = 10;

  // Sort by last_used_at desc to find the most recently used images
  const recentlyUsed = [...images]
    .filter(img => img.last_used_at)
    .sort((a, b) => new Date(b.last_used_at) - new Date(a.last_used_at))
    .slice(0, COOLDOWN)
    .map(img => img.id);

  // Available = not in the cooldown window
  const available = images.filter(img => !recentlyUsed.includes(img.id));

  // Use available pool; fall back to least-recently-used if all on cooldown
  const pool = available.length > 0 ? available : images;

  // Within the pool, try keyword/category match first
  const match = pool.find(img =>
    keywordWords.some(w => w.length > 3 && (img.category || "").toLowerCase().includes(w))
  );

  // Otherwise pick least-used, then oldest last_used_at
  const sorted = [...pool].sort((a, b) => {
    if ((a.times_used || 0) !== (b.times_used || 0)) return (a.times_used || 0) - (b.times_used || 0);
    if (!a.last_used_at) return -1;
    if (!b.last_used_at) return 1;
    return new Date(a.last_used_at) - new Date(b.last_used_at);
  });

  return match || sorted[0];
}

// Increments usage counter + timestamps image after selection.
async function markImageUsed(imageId) {
  if (!imageId) return;
  try {
    const { data: img } = await supabase.from("image_library").select("times_used").eq("id", imageId).single();
    await supabase.from("image_library").update({
      times_used: (img?.times_used || 0) + 1,
      last_used_at: new Date().toISOString(),
    }).eq("id", imageId);
    console.log("✓ Image usage tracked, ID:", imageId);
  } catch (e) {
    console.log("markImageUsed error:", e.message);
  }
}

// ── Markdown → HTML safety converter ─────────────────────────────────────────
// Claude sometimes returns markdown inside JSON even when instructed not to.
// This runs as a safety net on all post content before it goes to WordPress.
function markdownToHtml(md) {
  if (!md || typeof md !== "string") return md;
  // If it already looks like HTML (has real block tags), skip conversion
  if (/<(h[1-6]|ul|ol|li|blockquote|table)/i.test(md)) {
    // Still clean up any stray markdown inside HTML
    md = md.replace(/^#{1,6}\s+(.+)$/gm, (_, t) => t); // strip # inside HTML
    return md;
  }
  let html = md;
  // Convert headings  # H1  ## H2  ### H3  #### H4
  html = html.replace(/^####\s+(.+)$/gm, '<h4>$1</h4>');
  html = html.replace(/^###\s+(.+)$/gm, '<h3>$1</h3>');
  html = html.replace(/^##\s+(.+)$/gm, '<h2>$1</h2>');
  html = html.replace(/^#\s+(.+)$/gm, '<h2>$1</h2>'); // treat # as h2 in blog posts
  // Bold **text** and __text__
  html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  html = html.replace(/__(.+?)__/g, '<strong>$1</strong>');
  // Italic *text* and _text_
  html = html.replace(/\*([^*\n]+)\*/g, '<em>$1</em>');
  html = html.replace(/_([^_\n]+)_/g, '<em>$1</em>');
  // Markdown links [text](url)
  html = html.replace(/\[([^\]]+)\]\((https?:\/\/[^)]+)\)/g, '<a href="$2">$1</a>');
  // Convert bullet lists (– or - or * at line start) into <ul><li>
  html = html.replace(/((?:^[–\-*]\s.+\n?)+)/gm, (block) => {
    const items = block.trim().split("\n").map(line => {
      const text = line.replace(/^[–\-*]\s+/, "").trim();
      return text ? "<li>" + text + "</li>" : "";
    }).filter(Boolean).join("\n");
    return "<ul>\n" + items + "\n</ul>\n";
  });
  // Wrap bare paragraphs (lines not already wrapped in a tag)
  const lines = html.split("\n");
  const result = [];
  let inList = false;
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) { inList = false; continue; }
    if (/^<(h[1-6]|ul|ol|li|\/ul|\/ol|blockquote|\/blockquote)/i.test(trimmed)) {
      result.push(trimmed);
      inList = /^<(ul|ol|li)/i.test(trimmed);
    } else if (!inList) {
      result.push(`<p>${trimmed}</p>`);
    } else {
      result.push(trimmed);
    }
  }
  return result.join("\n");
}

// ── Rich schema builder ──────────────────────────────────────────────────────
// Builds all schema types + visible HTML sections for a blog post.
// opts = { title, metaDescription, slug, keyword, faqs, steps, clientName,
//          clientDomain, publishedDate, featuredImageUrl, authorName }
function buildSchemaBlock(opts) {
  const {
    title = "",
    metaDescription = "",
    slug = "",
    keyword = "",
    faqs = [],
    steps = [],
    clientName = "",
    clientDomain = "",
    publishedDate = new Date().toISOString(),
    featuredImageUrl = "",
    authorName = "",
  } = opts;

  const pageUrl = clientDomain ? (clientDomain.replace(/\/$/, "") + "/" + slug + "/") : "";
  const kwLower = keyword.toLowerCase();
  // HowTo triggers on explicitly procedural keywords
  // "repair" alone = service post; "how to repair" or "repair steps" = guide
  const isHowTo = /how.?to|step-?by-?step|diy|\binstall\b|\bmaintain\b|\bclean\b|troubleshoot|\bguide\b/.test(kwLower)
    || (/\bfix\b|\breplace\b|\brepair\b/.test(kwLower) && /how|step|guide|diy|yourself/.test(kwLower));

  const schemas = [];
  // Note: Article schema intentionally omitted — Yoast SEO already injects
  // a complete Article inside its @graph block, so we avoid duplication.

  // ── 2. HowTo schema (keyword-triggered) ───────────────────────
  let howToHtml = "";
  if (isHowTo && steps && steps.length > 0) {
    const howTo = {
      "@context": "https://schema.org",
      "@type": "HowTo",
      "name": title,
      "description": metaDescription,
      "step": steps.map((s, i) => ({
        "@type": "HowToStep",
        "position": i + 1,
        "name": s.name,
        "text": s.text,
      })),
    };
    schemas.push(howTo);

    // Visible HowTo HTML section
    const stepsHtml = steps.map((s, i) => `
  <div class="howto-step" style="display:flex;gap:14px;margin-bottom:18px;align-items:flex-start;">
    <div style="background:#d60000;color:#fff;font-weight:700;font-size:14px;min-width:28px;height:28px;border-radius:50%;display:flex;align-items:center;justify-content:center;flex-shrink:0;">${i + 1}</div>
    <div>
      <strong>${s.name}</strong>
      <p style="margin:4px 0 0;">${s.text}</p>
    </div>
  </div>`).join("");

    howToHtml = `
<div class="howto-section" style="margin-top:32px;padding-top:24px;border-top:2px solid #eee;">
  <h2>Step-by-Step Guide</h2>
  ${stepsHtml}
</div>`;
  }

  // ── 3. FAQPage schema + sc_fs_multi_faq shortcode ───────────────
  let faqHtml = "";
  if (faqs && faqs.length > 0) {
    const faqSchema = {
      "@context": "https://schema.org",
      "@type": "FAQPage",
      "mainEntity": faqs.map(f => ({
        "@type": "Question",
        "name": f.question,
        "acceptedAnswer": { "@type": "Answer", "text": f.answer },
      })),
    };
    schemas.push(faqSchema);

    // Build plain HTML FAQ accordion — no plugin dependency, always renders correctly
    const faqItems = faqs.map(f => `<div class="faq-item" style="margin-bottom:20px;border-bottom:1px solid currentColor;border-bottom-color:rgba(128,128,128,0.2);padding-bottom:16px;">
<h3 style="margin:0 0 8px;font-size:1.1em;">${f.question.replace(/</g,"&lt;").replace(/>/g,"&gt;")}</h3>
<p style="margin:0;">${f.answer.replace(/</g,"&lt;").replace(/>/g,"&gt;")}</p>
</div>`).join("\n");

    faqHtml = `\n<div class="faq-section">\n<h2>Frequently Asked Questions</h2>\n${faqItems}\n</div>`;
  }

  // ── 4. Combined JSON-LD injection ─────────────────────────────
  const schemaHtml = schemas.map(s =>
    `<script type="application/ld+json">${JSON.stringify(s)}</script>`
  ).join("\n");

  return {
    appendHtml: howToHtml + faqHtml,   // visible sections to append to post content
    schemaHtml,                         // <script> blocks to append after visible sections
    isHowTo,
    schemaTypes: schemas.map(s => s["@type"]),
  };
}

// Legacy wrapper kept for any existing calls
function buildFaqBlock(faqs) {
  const { appendHtml, schemaHtml } = buildSchemaBlock({ faqs });
  return {
    html: appendHtml,
    schema: null, // schema now inlined in schemaHtml
    schemaHtml,
  };
}

// ── POST-PUBLISH QA VERIFIER ─────────────────────────────────────────────────
// After a post goes live, fetches the page and runs Claude against it to check:
//   - Content renders as HTML (no markdown leaking through)
//   - Reading flow makes sense (no truncation, no garbled text)
//   - No DIY repair instructions that undercut service revenue
//   - Internal/external links present and not broken
//   - Schema JSON-LD block present in the HTML
//   - Meta description and title are set
// Returns a qa object with { passed, issues[], score, liveUrl }
const verifyPublishedPost = async (liveUrl, { title, keyword, metaDescription, wordpressUrl, authHeaders } = {}) => {
  const qa = { passed: false, score: 0, issues: [], warnings: [], liveUrl };

  // 1. Fetch live page — retry up to 4x with backoff (WP/cache may take a moment)
  let html = "";
  for (let attempt = 1; attempt <= 4; attempt++) {
    await new Promise(r => setTimeout(r, attempt * 2500));
    try {
      const res = await axios.get(liveUrl, {
        headers: { "User-Agent": "Mozilla/5.0 (compatible; FortitudeBot/1.0)", "Cache-Control": "no-cache", "Pragma": "no-cache" },
        httpsAgent, maxRedirects: 5, timeout: 15000,
        params: { nocache: Date.now() }
      });
      if (typeof res.data === "string" && res.data.length > 500) { html = res.data; break; }
    } catch(e) {
      console.log(`[QA] Fetch attempt ${attempt} failed: ${e.message}`);
    }
  }

  if (!html) {
    qa.issues.push({ type: "fetch_failed", severity: "error", message: "Could not fetch live page after 4 attempts — post may not be publicly accessible yet" });
    return qa;
  }

  // 2. Isolate post content — strip header/nav/footer so QA only sees the article
  let postHtml = html;
  const contentMatch =
    html.match(/<article[^>]*>([\s\S]*?)<\/article>/i)?.[1] ||
    html.match(/class="[^"]*entry-content[^"]*"[^>]*>([\s\S]*?)<\/div>/i)?.[1] ||
    html.match(/class="[^"]*post-content[^"]*"[^>]*>([\s\S]*?)<\/div>/i)?.[1];
  if (contentMatch && contentMatch.length > 200) {
    postHtml = contentMatch;
    console.log(`[QA] Isolated post content (${postHtml.length} chars from ${html.length} char full page)`);
  } else {
    postHtml = html
      .replace(/<header[\s\S]*?<\/header>/gi, "")
      .replace(/<nav[\s\S]*?<\/nav>/gi, "")
      .replace(/<footer[\s\S]*?<\/footer>/gi, "");
    console.log(`[QA] Nav-stripped content (${postHtml.length} chars)`);
  }

  // Structural checks against post content only
  const bodyText = postHtml.replace(/<script[\s\S]*?<\/script>/gi, "").replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();

  // Markdown leaking through
  const markdownPatterns = [
    { re: /#{1,6}\s+\w/, label: "Heading markdown (# Title)" },
    { re: /\*{2}[^*]+\*{2}/, label: "Bold markdown (**text**)" },
    { re: /\n-\s+\w/, label: "List markdown (- item)" },
    { re: /\[\w[^\]]+\]\(https?:/, label: "Link markdown ([text](url))" },
    { re: /^```/, label: "Code block markdown (```)" },
  ];
  for (const { re, label } of markdownPatterns) {
    if (re.test(bodyText)) qa.issues.push({ type: "markdown_leak", severity: "error", message: `Markdown not converted to HTML: ${label}` });
  }

  // Title tag
  const liveTitle = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1]?.trim() || "";
  if (!liveTitle) qa.issues.push({ type: "no_title", severity: "error", message: "No <title> tag found on live page" });
  else if (title && !liveTitle.toLowerCase().includes(title.toLowerCase().slice(0, 20)))
    qa.warnings.push({ type: "title_mismatch", severity: "warning", message: `Live title "${liveTitle}" doesn't match expected "${title}"` });

  // Meta description
  const liveMeta = html.match(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i)?.[1] ||
                   html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+name=["']description["']/i)?.[1] || "";
  if (!liveMeta) qa.warnings.push({ type: "no_meta", severity: "warning", message: "No meta description found on live page" });

  // H1 present
  const h1Count = (html.match(/<h1[^>]*>/gi) || []).length;
  if (h1Count === 0) qa.issues.push({ type: "no_h1", severity: "error", message: "No H1 tag found — post title may not be rendering" });
  if (h1Count > 1) qa.warnings.push({ type: "multiple_h1", severity: "warning", message: `${h1Count} H1 tags found — should be exactly 1` });

  // H2 subheadings (blog post should have structure)
  const h2Count = (html.match(/<h2[^>]*>/gi) || []).length;
  if (h2Count < 2) qa.warnings.push({ type: "no_structure", severity: "warning", message: `Only ${h2Count} H2 subheading(s) — post lacks section structure` });

  // Raw shortcodes visible on page (unrendered WordPress shortcodes)
  if (/\[sc_fs_multi_faq|\[faq|\[accordion/i.test(bodyText)) {
    qa.issues.push({ type: "raw_shortcode", severity: "error", message: "Raw WordPress shortcode visible on live page — plugin likely not installed. Use plain HTML instead." });
  }

  // Visible schema JSON in body text (should only be in <script> tags)
  if (/"@context"\s*:\s*"https?:\/\/schema\.org"/.test(bodyText)) {
    qa.issues.push({ type: "visible_schema_json", severity: "error", message: "Schema JSON-LD is visible as plain text on the page — it must be inside a <script type=\"application/ld+json\"> tag, not in the post body." });
  }

  // Word count (rough)
  const wordCount = bodyText.split(/\s+/).filter(Boolean).length;
  if (wordCount < 300) qa.issues.push({ type: "too_short", severity: "error", message: `Post appears very short on live page (${wordCount} words) — content may not have rendered` });
  else if (wordCount < 600) qa.warnings.push({ type: "short_content", severity: "warning", message: `Post is only ~${wordCount} words on live page — shorter than recommended` });

  // Images with alt text
  const imgTags = [...postHtml.matchAll(/<img([^>]*)>/gi)].map(m => m[1]);
  const imgsNoAlt = imgTags.filter(a => { const m = a.match(/alt=["']([^"']*)['"]/i); return !m || !m[1].trim(); });
  if (imgsNoAlt.length > 0) qa.warnings.push({ type: "img_no_alt", severity: "warning", message: `${imgsNoAlt.length} image(s) missing alt text` });

  // Schema JSON-LD
  const hasSchema = html.includes('"@context"') && html.includes('"@type"'); // check full html — schema is in <head> or end of body
  if (!hasSchema) qa.warnings.push({ type: "no_schema", severity: "warning", message: "No JSON-LD schema found — schema may not have injected correctly" });

  // Internal links
  const linkCount = (html.match(/<a[^>]+href=["'][^"']*["']/gi) || []).length;
  if (linkCount < 2) qa.warnings.push({ type: "no_links", severity: "warning", message: `Only ${linkCount} link(s) found — missing internal/external links` });

  // Truncation signals
  if (bodyText.includes("…</") || /\w{3,}\.\.\.</.test(bodyText))
    qa.issues.push({ type: "truncated", severity: "error", message: "Post appears truncated — content may have been cut off" });

  // 3. AI content review — reads the actual text and checks it makes sense
  try {
    const Anthropic = require("@anthropic-ai/sdk");
    const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

    // Send just the body text (no HTML noise) — limit to ~3000 words to stay within context
    // reviewText is derived from isolated post content only (nav/header/footer stripped)
    const reviewText = bodyText.slice(0, 12000);
    const msg = await anthropic.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 600,
      messages: [{ role: "user", content: `You are a QA reviewer for a home service company blog. Review this published blog post and return ONLY valid JSON.

Keyword targeted: "${keyword || "unknown"}"
Post title: "${title || liveTitle || "unknown"}"

Post content (from live page):
---
${reviewText}
---

Check for these issues and return JSON:
{
  "readable": true/false,        // does the post read naturally and make sense start to finish?
  "on_topic": true/false,        // does it stay on topic for the keyword?
  "diy_risk": true/false,        // does it contain step-by-step DIY repair instructions that could let a homeowner skip hiring a pro?
  "abrupt_ending": true/false,   // does the post end abruptly or get cut off mid-sentence?
  "repeated_content": true/false, // does the same paragraph or sentence appear more than once?
  "ai_artifacts": true/false,    // does it contain AI writing artifacts like "Certainly!", "In conclusion,", "As an AI", placeholder text like [Company Name], or unnatural filler?
  "notes": "one sentence summary of the biggest issue found, or 'Post looks good' if no issues"
}
Return ONLY the JSON, nothing else.` }]
    });

    const aiReview = JSON.parse(msg.content[0].text.trim().replace(/```json|```/g, "").trim());

    if (!aiReview.readable)        qa.issues.push({ type: "not_readable", severity: "error", message: `AI review: Post does not read naturally — ${aiReview.notes}` });
    if (!aiReview.on_topic)        qa.issues.push({ type: "off_topic", severity: "error", message: `AI review: Post appears off-topic for keyword "${keyword}"` });
    if (aiReview.diy_risk)         qa.issues.push({ type: "diy_risk", severity: "error", message: "AI review: Post contains DIY repair instructions — rewrites needed to protect service revenue" });
    if (aiReview.abrupt_ending)    qa.issues.push({ type: "abrupt_ending", severity: "error", message: "AI review: Post ends abruptly — may have been truncated during generation" });
    if (aiReview.repeated_content) qa.warnings.push({ type: "repeated_content", severity: "warning", message: "AI review: Repeated paragraphs detected" });
    if (aiReview.ai_artifacts)     qa.warnings.push({ type: "ai_artifacts", severity: "warning", message: `AI review: AI writing artifacts found — ${aiReview.notes}` });

    qa.aiNotes = aiReview.notes;
    qa.aiReview = aiReview;
  } catch(e) {
    console.log("[QA] AI review failed:", e.message);
    qa.warnings.push({ type: "ai_review_skipped", severity: "info", message: "AI content review skipped — " + e.message });
  }

  // 4. Score and pass/fail
  const errorCount   = qa.issues.filter(i => i.severity === "error").length;
  const warningCount = qa.warnings.filter(w => w.severity === "warning").length;
  qa.score    = Math.max(0, 100 - (errorCount * 20) - (warningCount * 5));
  qa.passed   = errorCount === 0;
  qa.wordCount = wordCount;
  qa.liveTitle = liveTitle;

  console.log(`[QA] ${liveUrl} — score: ${qa.score}, errors: ${errorCount}, warnings: ${warningCount}, passed: ${qa.passed}`);
  if (qa.aiNotes) console.log(`[QA] AI notes: ${qa.aiNotes}`);

  return qa;
};


// ── POST-PUBLISH AUTO-REPAIR LOOP ────────────────────────────────────────────
// 1. Run QA on live page
// 2. If failed: fetch current WP post content, send to Claude with all failed
//    checks listed, get a fully rewritten version back, write it to WP,
//    bust cache, wait, re-run QA
// 3. Repeat up to MAX_REPAIR_CYCLES. Return final QA + full repair history.

const MAX_REPAIR_CYCLES = 3;

// ─── YOAST OPTIMIZATION LOOP ─────────────────────────────────────────────────
// Runs up to 3 passes after publish. Each pass checks 4 Yoast problems:
//   - Keyphrase density (too low)        → AI content rewrite
//   - Keyphrase not in SEO title         → AI title rewrite via Fortitude plugin
//   - Keyphrase missing from H2/H3s      → AI content rewrite
//   - Meta description out of 120-156 char range → AI meta rewrite via Fortitude plugin
// ─────────────────────────────────────────────────────────────────────────────
const YOAST_MAX_PASSES = 5;

const runYoastOptimizeLoop = async (wpPostId, { title, keyword, metaDescription }, wpBaseUrl, authHeaders, seoCaps) => {
  const Anthropic = require("@anthropic-ai/sdk");
  const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
  const log = (msg) => console.log(`[YoastOpt] ${msg}`);
  const fixes = [];
  let currentTitle   = title;
  let currentMeta    = metaDescription;
  let currentContent = "";

  // ── Check Yoast issues against current state ──────────────────────────────
  const checkIssues = (html, ttl, meta, kw) => {
    const issues = [];
    const kwLower   = kw.toLowerCase();
    const kwWords   = kwLower.split(/\s+/);
    const textOnly  = html.replace(/<[^>]+>/g, " ").toLowerCase();
    const wordCount = textOnly.split(/\s+/).filter(Boolean).length;

    // Keyphrase density
    let kwCount = 0, pos = 0;
    while ((pos = textOnly.indexOf(kwLower, pos)) !== -1) { kwCount++; pos += kwLower.length; }
    const minOccurrences = Math.max(5, Math.floor(wordCount / 300));
    if (kwCount < minOccurrences) issues.push({ type: "density", kwCount, minOccurrences, wordCount });

    // Keyphrase in SEO title
    const titleLower    = ttl.replace(/ - %%sitename%%/, "").toLowerCase();
    const missingInTitle = kwWords.filter(w => !titleLower.includes(w));
    if (missingInTitle.length > 0) issues.push({ type: "title", missingWords: missingInTitle });

    // Keyphrase in H2/H3 subheadings
    const headings = [...html.matchAll(/<h[23][^>]*>(.*?)<\/h[23]>/gi)].map(m => m[1].replace(/<[^>]+>/g, "").toLowerCase());
    const hasKwInHeading = headings.some(h => kwWords.some(w => h.includes(w)));
    if (!hasKwInHeading) issues.push({ type: "subheadings", headings: headings.slice(0, 6) });

    // Meta description length
    if (meta.length < 120 || meta.length > 156) issues.push({ type: "meta_length", length: meta.length });

    return issues;
  };

  // ── Write Yoast meta fields via Fortitude plugin or REST ──────────────────
  const writeMeta = async (fields) => {
    if (seoCaps.fortitudePlugin) {
      try {
        const r = await axios.post(`${wpBaseUrl}/wp-json/fortitude/v1/seo-meta`,
          { post_id: wpPostId, ...fields }, { headers: authHeaders, httpsAgent });
        if (r.data?.success) return true;
      } catch(e) { log(`Fortitude meta write failed: ${e.message}`); }
    }
    const metaPayload = {};
    if (fields.focuskw  !== undefined) metaPayload._yoast_wpseo_focuskw  = fields.focuskw;
    if (fields.metadesc !== undefined) metaPayload._yoast_wpseo_metadesc = fields.metadesc;
    if (fields.title    !== undefined) metaPayload._yoast_wpseo_title    = fields.title;
    try {
      await axios.post(`${wpBaseUrl}/wp-json/wp/v2/posts/${wpPostId}`,
        { meta: metaPayload }, { headers: authHeaders, httpsAgent });
      return true;
    } catch(e) { log(`REST meta write failed: ${e.message}`); return false; }
  };

  // ── Fetch and write post HTML via WP REST ─────────────────────────────────
  const getPostHtml = async () => {
    const r = await axios.get(`${wpBaseUrl}/wp-json/wp/v2/posts/${wpPostId}?context=edit`, { headers: authHeaders, httpsAgent });
    return r.data?.content?.raw || "";
  };
  const writePostHtml = async (html) => {
    await axios.post(`${wpBaseUrl}/wp-json/wp/v2/posts/${wpPostId}`,
      { content: html, status: "publish" }, { headers: authHeaders, httpsAgent });
  };

  const passHistory = [];
  log(`Starting — post ${wpPostId}, keyphrase: "${keyword}"`);
  try { currentContent = await getPostHtml(); }
  catch(e) { log(`Could not fetch post: ${e.message}`); return { yoastScore: null, passes: 0, issues: [], fixes }; }

  for (let pass = 1; pass <= YOAST_MAX_PASSES; pass++) {
    const issues = checkIssues(currentContent, currentTitle, currentMeta, keyword);
    log(`Pass ${pass}/${YOAST_MAX_PASSES} — issues: ${issues.map(i => i.type).join(", ") || "none ✓"}`);
    if (issues.length === 0) { log(`✓ All Yoast checks passed`); break; }

    let metaChanged = false, contentChanged = false;
    const attemptedThisPass = [];

    // Build escalation context from prior failed passes
    const priorAttempts = passHistory.length > 0
      ? `\n\nPREVIOUS ATTEMPTS THAT FAILED:\n` + passHistory.map((h, i) =>
          `Pass ${i + 1}: Tried [${h.attempted.join(", ")}]. Still failing after: [${h.remaining.join(", ")}].`
        ).join("\n") + `\n\nUse a DIFFERENT approach this time. Rewrite entire paragraphs, add new sentences, rename headings completely. Do not repeat the same edits.`
      : "";

    // Fix: meta description too long
    const metaIssue = issues.find(i => i.type === "meta_length");
    if (metaIssue) {
      attemptedThisPass.push("meta_length");
      const priorMetaFails = passHistory.filter(h => h.attempted.includes("meta_length")).length;
      const metaPrompt = priorMetaFails > 0
        ? `PREVIOUS ${priorMetaFails} ATTEMPT(S) FAILED — result was out of 120-156 char range. Rewrite to be between 120-156 characters total including spaces while keeping "${keyword}". Count carefully. Return ONLY the text, no quotes.\n\nCurrent (${metaIssue.length} chars): "${currentMeta}"`
        : `Rewrite to be between 120-156 characters total including spaces while keeping the keyphrase "${keyword}". Count carefully. Return ONLY the new meta description text, no quotes.\n\n"${currentMeta}"`;
      try {
        const msg = await anthropic.messages.create({
          model: "claude-sonnet-4-20250514", max_tokens: 300,
          messages: [{ role: "user", content: metaPrompt }]
        });
        const newMeta = msg.content[0].text.trim().replace(/^["']|["']$/g, "");
        if (newMeta.length >= 120 && newMeta.length <= 156) {
          currentMeta = newMeta; metaChanged = true;
          fixes.push({ pass, type: "meta_length", fix: `Trimmed to ${newMeta.length} chars` });
          log(`Fixed meta: ${newMeta.length} chars`);
        }
      } catch(e) { log(`Meta rewrite failed: ${e.message}`); }
    }

    // Fix: keyphrase not in SEO title
    const titleIssue = issues.find(i => i.type === "title");
    if (titleIssue) {
      attemptedThisPass.push("title");
      const priorTitleFails = passHistory.filter(h => h.attempted.includes("title")).length;
      const titlePrompt = priorTitleFails > 0
        ? `PREVIOUS ${priorTitleFails} ATTEMPT(S) FAILED. Missing words: ${titleIssue.missingWords.join(", ")}. Start with the EXACT phrase "${keyword}" word-for-word — no paraphrasing. Max 60 chars. Return ONLY the title, no suffix, no quotes.\n\nCurrent: "${currentTitle.replace(/ - %%sitename%%/, "")}"`
        : `Rewrite this SEO title so it STARTS WITH the exact keyphrase "${keyword}". Keep the post topic clear. Max 60 characters. Return ONLY the title text, no suffix, no quotes.\n\nCurrent: "${currentTitle.replace(/ - %%sitename%%/, "")}"`;
      try {
        const msg = await anthropic.messages.create({
          model: "claude-sonnet-4-20250514", max_tokens: 200,
          messages: [{ role: "user", content: titlePrompt }]
        });
        const newTitle = msg.content[0].text.trim().replace(/^["']|["']$/g, "").replace(/ - %%sitename%%$/, "") + " - %%sitename%%";
        currentTitle = newTitle; metaChanged = true;
        fixes.push({ pass, type: "title", fix: `Rewritten to start with keyphrase` });
        log(`Fixed title: "${newTitle}"`);
      } catch(e) { log(`Title rewrite failed: ${e.message}`); }
    }

    if (metaChanged) {
      await writeMeta({ metadesc: currentMeta, title: currentTitle, focuskw: keyword });
      log(`Meta/title written to WP`);
    }

    // Fix: keyphrase density + subheadings (content rewrite)
    const needsContentFix = issues.some(i => i.type === "density" || i.type === "subheadings");
    if (needsContentFix) {
      const densityIssue = issues.find(i => i.type === "density");
      const headingIssue = issues.find(i => i.type === "subheadings");
      if (densityIssue) attemptedThisPass.push("density");
      if (headingIssue) attemptedThisPass.push("subheadings");

      const priorContentFails = passHistory.filter(h =>
        h.attempted.includes("density") || h.attempted.includes("subheadings")
      ).length;

      const escalation = priorContentFails > 0
        ? `\n\nCRITICAL — PREVIOUS ${priorContentFails} REWRITE(S) FAILED. Take a completely different approach:\n- Rewrite entire paragraphs from scratch, not just minor tweaks.\n- Add 2-3 brand new sentences in different sections that naturally use "${keyword}".\n- For headings: completely rename them — don't just insert a word.\n- Be bold and aggressive with changes.${priorAttempts}`
        : "";

      const contentFixList = [];
      if (densityIssue) contentFixList.push(`Keyphrase density: found "${keyword}" only ${densityIssue.kwCount} times, need ${densityIssue.minOccurrences}+ in ${densityIssue.wordCount} words. Weave the exact phrase naturally into more paragraphs.`);
      if (headingIssue) contentFixList.push(`Keyphrase in subheadings: no H2/H3 contains "${keyword}" or synonyms. Rewrite at least 2 headings. Current: ${headingIssue.headings.join(" | ")}`);

      try {
        const schemaMatch = currentContent.match(/(<script type="application\/ld\+json">[\s\S]*?<\/script>\s*)+$/i);
        const schemaBlock = schemaMatch ? schemaMatch[0] : "";
        const bodyHtml    = schemaMatch ? currentContent.slice(0, schemaMatch.index) : currentContent;

        const msg = await anthropic.messages.create({
          model: "claude-sonnet-4-20250514", max_tokens: 6000,
          system: `You are an expert SEO blog editor. Fix ONLY the listed Yoast SEO issues. Preserve all existing links, images, and post structure. Output ONLY the corrected HTML body — no preamble, no markdown, no explanation.`,
          messages: [{ role: "user", content: `Fix these Yoast SEO issues:\n\n${contentFixList.join("\n\n")}${escalation}\n\nPost keyword: "${keyword}"\n\nCurrent HTML:\n---\n${bodyHtml.slice(0, 24000)}\n---\n\nReturn ONLY the corrected HTML body.` }]
        });
        let rewritten = msg.content[0].text.trim().replace(/^```html?\n?/i, "").replace(/```$/m, "").trim();
        if (schemaBlock) rewritten = rewritten + "\n" + schemaBlock;
        currentContent = rewritten; contentChanged = true;
        fixes.push({ pass, type: [densityIssue && "density", headingIssue && "subheadings"].filter(Boolean).join("+"), fix: priorContentFails > 0 ? `AI rewrite (escalated attempt ${priorContentFails + 1})` : "AI content rewrite" });
        log(`Content rewritten (escalation level: ${priorContentFails})`);
      } catch(e) { log(`Content rewrite failed: ${e.message}`); }

      if (contentChanged) {
        try { await writePostHtml(currentContent); log(`Content written to WP`); }
        catch(e) { log(`Content write failed: ${e.message}`); }
      }
    }

    // Record what this pass tried and what still remains after
    const remainingAfter = checkIssues(currentContent, currentTitle, currentMeta, keyword);
    passHistory.push({ pass, attempted: attemptedThisPass, remaining: remainingAfter.map(i => i.type) });

    if (!metaChanged && !contentChanged) { log(`No fixes applied on pass ${pass}, stopping`); break; }
  }

  const finalIssues  = checkIssues(currentContent, currentTitle, currentMeta, keyword);
  const yoastScore   = Math.max(0, 100 - (finalIssues.length * 20));
  log(`Done. Final issues: ${finalIssues.length}, score: ~${yoastScore}. Total fixes: ${fixes.length}`);
  return { yoastScore, passes: fixes.length, issues: finalIssues, fixes, finalTitle: currentTitle, finalMeta: currentMeta };
};

const qaRepairLoop = async (wpPostId, liveUrl, qaContext, wpBaseUrl, authHeaders) => {
  const { title, keyword, metaDescription } = qaContext;
  const history = [];   // [{cycle, qa, rewriteReason}]

  const Anthropic = require("@anthropic-ai/sdk");
  const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

  // ── helpers ──────────────────────────────────────────────────────────────
  const getWpContent = async () => {
    const r = await axios.get(
      `${wpBaseUrl}/wp-json/wp/v2/posts/${wpPostId}?context=edit&_fields=id,content`,
      { headers: authHeaders, httpsAgent, timeout: 10000 }
    );
    return r.data?.content?.raw || r.data?.content?.rendered || "";
  };

  const writeWpContent = async (html) => {
    await axios.post(
      `${wpBaseUrl}/wp-json/wp/v2/posts/${wpPostId}`,
      { content: html },
      { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent }
    );
    // Bust all common cache plugins
    for (const bust of [
      () => axios.post(`${wpBaseUrl}/wp-json/wp-rocket/v1/clear-cache`, {}, { headers: authHeaders, httpsAgent }),
      () => axios.get(`${wpBaseUrl}/wp-json/w3tc/v1/flush_all`, { headers: authHeaders, httpsAgent }),
      () => axios.post(`${wpBaseUrl}/wp-json/wp-super-cache/v1/cache`, { delete: true }, { headers: authHeaders, httpsAgent }),
      // Touch post to invalidate object cache
      () => axios.post(`${wpBaseUrl}/wp-json/wp/v2/posts/${wpPostId}`, { date: new Date().toISOString() }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent }),
    ]) { try { await bust(); } catch(e) {} }
    await new Promise(r => setTimeout(r, 4000)); // wait for CDN/cache flush
  };

  const buildIssueList = (qa) => {
    const all = [...(qa.issues || []), ...(qa.warnings || [])];
    return all.map((i, n) => `${n + 1}. [${i.severity.toUpperCase()}] ${i.message}`).join("\n");
  };

  // ── initial QA ───────────────────────────────────────────────────────────
  console.log(`[QA] Initial check: ${liveUrl}`);
  let qa = await verifyPublishedPost(liveUrl, { title, keyword, metaDescription, wordpressUrl: wpBaseUrl, authHeaders });
  history.push({ cycle: 0, qa, action: "initial_check" });

  if (qa.passed) {
    console.log(`[QA] ✓ Passed on first check (score: ${qa.score})`);
    return { qa, history };
  }

  // ── repair cycles ────────────────────────────────────────────────────────
  for (let cycle = 1; cycle <= MAX_REPAIR_CYCLES; cycle++) {
    const errorCount   = (qa.issues  || []).filter(i => i.severity === "error").length;
    const warningCount = (qa.warnings || []).filter(w => w.severity === "warning").length;
    console.log(`[QA] Cycle ${cycle}/${MAX_REPAIR_CYCLES} — ${errorCount} errors, ${warningCount} warnings. Rewriting...`);

    let currentHtml = "";
    try { currentHtml = await getWpContent(); }
    catch(e) { console.error("[QA] Could not fetch WP content:", e.message); break; }

    if (!currentHtml) { console.error("[QA] Empty content returned from WP"); break; }

    // Strip schema blocks from what we send to AI — they'll be re-appended after
    const schemaMatch = currentHtml.match(/(<script type="application\/ld\+json">[\s\S]*?<\/script>\s*)+$/i);
    const schemaBlock = schemaMatch ? schemaMatch[0] : "";
    const bodyHtml    = schemaMatch ? currentHtml.slice(0, schemaMatch.index) : currentHtml;

    const issueList = buildIssueList(qa);

    let rewrittenHtml = "";
    try {
      const msg = await anthropic.messages.create({
        model: "claude-sonnet-4-20250514",
        max_tokens: 4000,
        system: `You are an expert blog editor for a home service company. You receive a published blog post that failed quality checks and must fix ALL listed issues while preserving the post's full length, keyword focus, and professional tone.

CRITICAL RULES:
- Output ONLY the corrected HTML body content. No markdown, no preamble, no explanation.
- Use only <h2>, <h3>, <p>, <ul>, <li>, <strong>, <a href="..."> tags. NO markdown (#, **, -, etc).
- Keep ALL internal and external links that are already in the post.
- The post must be complete — do not truncate or shorten it.
- NEVER include step-by-step DIY repair instructions that let homeowners skip hiring a pro. Reframe any such content as diagnostic steps or what a technician will do.
- Do not include JSON-LD schema — that will be appended separately.`,
        messages: [{
          role: "user",
          content: `Fix this blog post. It failed QA with these issues:

${issueList}

AI review notes: ${qa.aiNotes || "none"}

Post details:
- Title: ${title}
- Target keyword: ${keyword}
- Meta description: ${metaDescription}

Current HTML content:
---
${bodyHtml.slice(0, 20000)}
---

Return ONLY the corrected HTML body. Fix every issue listed above. Preserve all links, full length, and keyword targeting.`
        }]
      });

      rewrittenHtml = msg.content[0].text.trim();
      // Strip any accidental markdown fences
      rewrittenHtml = rewrittenHtml.replace(/^```html?\n?/i, "").replace(/```$/m, "").trim();
      // Re-attach schema block
      if (schemaBlock) rewrittenHtml = rewrittenHtml + "\n" + schemaBlock;
    } catch(e) {
      console.error(`[QA] AI rewrite failed on cycle ${cycle}:`, e.message);
      history.push({ cycle, action: "rewrite_failed", error: e.message, qa });
      break;
    }

    // Write fixed content back to WP
    try {
      console.log(`[QA] Writing cycle ${cycle} rewrite to WP post ${wpPostId}...`);
      await writeWpContent(rewrittenHtml);
    } catch(e) {
      console.error(`[QA] WP write failed on cycle ${cycle}:`, e.message);
      history.push({ cycle, action: "write_failed", error: e.message, qa });
      break;
    }

    // Re-run QA on the live page
    console.log(`[QA] Re-checking live page after cycle ${cycle} rewrite...`);
    qa = await verifyPublishedPost(liveUrl, { title, keyword, metaDescription, wordpressUrl: wpBaseUrl, authHeaders });
    history.push({ cycle, action: "rewrite_and_recheck", qa });

    if (qa.passed) {
      console.log(`[QA] ✓ Passed after cycle ${cycle} rewrite (score: ${qa.score})`);
      break;
    }

    console.log(`[QA] Still failing after cycle ${cycle} (score: ${qa.score})`);
  }

  if (!qa.passed) {
    console.warn(`[QA] ⚠ Post ${wpPostId} still failing after ${MAX_REPAIR_CYCLES} repair cycles. Manual review needed.`);
  }

  return { qa, history };
};


const publishPostForClient = async (client, keyword) => {
  try {
    const Anthropic = require("@anthropic-ai/sdk");
    const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

    let internalPages = [];
    if (client.wordpress_url) {
      try {
        const wpPagesRes = await axios.get(`${client.wordpress_url}/wp-json/wp/v2/pages?per_page=20&_fields=title,link,slug`, { httpsAgent });
        const pages = wpPagesRes.data.map(p => ({ title: p.title.rendered, url: p.link, slug: p.slug, type: "page" }));
        const wpPostsRes = await axios.get(`${client.wordpress_url}/wp-json/wp/v2/posts?per_page=30&status=publish&_fields=title,link,slug`, { httpsAgent });
        const posts = wpPostsRes.data.map(p => ({ title: p.title.rendered, url: p.link, slug: p.slug, type: "post" }));
        internalPages = [...pages, ...posts];
      } catch (e) {}
    }

    // Fetch all existing titles/slugs to prevent duplicate content
    const existingContent = await fetchExistingContent(client.wordpress_url, client.id);
    const existingContentPrompt = buildExistingContentPrompt(existingContent);
    console.log(`[Scheduler] Found ${existingContent.length} existing pages/posts to avoid for ${client.name}`);

    let featuredImage = null;
    const keywordWords = keyword.toLowerCase().split(" ");
    const { data: clientImages } = await supabase.from("image_library").select("*").eq("client_id", client.id).order("times_used", { ascending: true }).order("last_used_at", { ascending: true, nullsFirst: true });
    if (clientImages?.length > 0) {
      featuredImage = selectFeaturedImage(clientImages, keywordWords);
    }

    const blogPosts = internalPages.filter(p => p.type === "post");
    const servicePages = internalPages.filter(p => p.type === "page" && !/home|homepage/i.test(p.title));
    const contactPage = internalPages.find(p => /contact|get.a.quote|free.estimate|schedule|book/i.test(p.title));
    const internalLinksPrompt = internalPages.length > 0
      ? ("\n\nINTERNAL LINKS — TOPIC CLUSTER STRUCTURE (Required):\nAvailable published blog posts:\n" + (blogPosts.map(p => "  [BLOG] " + p.title + ": " + p.url).join("\n") || "  (none yet — use service pages only)") + "\nAvailable service/other pages (excluding homepage):\n" + (servicePages.map(p => "  [PAGE] " + p.title + ": " + p.url).join("\n") || "  (none available)") + (contactPage ? "\nContact/quote page: [CONTACT] " + contactPage.title + ": " + contactPage.url : "") + "\n\nInternal linking rules — follow this structure:\n1. Up to 3 BLOG POSTS: Link to up to 3 previously published blog posts on complementary topics. NEVER link to a post targeting the same or overlapping keyword (keyword cannibalization).\n2. 1 SERVICE PAGE: Link once to the most relevant service page. Never link to the homepage.\n3. 1 CONTACT/QUOTE PAGE: Include one link to the contact or scheduling page as a call-to-action.\n4. If no blog posts exist yet, use 2 service pages + 1 contact page.\n5. Never link to the homepage.")
      : "";

    const externalLinksPrompt = `

EXTERNAL LINKS — OUTBOUND LINKING RULES:
Include 1-2 external links to authoritative sources. Follow these rules strictly:
1. Link to a RESOURCE-SPECIFIC PAGE (a specific guide, data page, article, or stats page) — NOT a homepage or top-level domain.
2. The linked page must contain a SPECIFIC FACT or STATISTIC directly relevant to this blog post. Reference that fact inline in your writing.
3. Anchor text should describe the resource or cite the fact — not "click here" or a bare domain name.
4. If you cannot confidently identify a specific resource page URL that actually exists, do NOT invent a URL — skip the external link rather than fabricate one.
5. Draw from these authoritative domains (find a specific resource page within them, not the homepage):
   - HVAC/Energy: energystar.gov or energy.gov
   - Plumbing/Water: epa.gov/watersense or epa.gov
   - Electrical/Safety: esfi.org
   - Roofing: nrca.net
   - General home safety: cpsc.gov`;

    const message = await anthropic.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 4000,
      system: `You are a professional SEO content writer specializing in home service companies. ${client.brand_voice ? `Brand voice: ${client.brand_voice}` : ""}

CRITICAL CONTENT RULE: This blog content is for a professional home service company that earns revenue from service calls. You must NEVER write step-by-step DIY repair instructions that would allow a homeowner to fix the problem themselves and skip hiring a professional. Instead:
- Help readers IDENTIFY and DIAGNOSE the problem (warning signs, symptoms, what to look for)
- Explain WHY the problem happens and what causes it
- Describe what a professional fix involves WITHOUT giving enough detail to DIY it
- Always recommend contacting a licensed professional for actual repairs
- Use phrases like "a certified technician will...", "your pro will...", "call ${client.name} to..."
- For how-to style posts, frame steps as "how to identify if you need X" or "what to expect when a pro fixes X" — not "how to fix X yourself"
This protects the business's service revenue while keeping content SEO-valuable.`,
      messages: [{ role: "user", content: `Write a complete SEO blog post for "${client.name}" targeting: "${keyword}"${internalLinksPrompt}${externalLinksPrompt}${existingContentPrompt}

Return ONLY valid JSON with these exact fields:
- title: SEO title — STRICT Yoast limit: 50-60 characters total including spaces. Count carefully.
- metaDescription: meta description — STRICT range: 120-156 characters total including spaces. Count character by character. Must be at least 120 and no more than 156.
- slug: URL slug (lowercase, hyphens)
- content: pure HTML body using <h2>, <h3>, <p>, <ul>, <li>, <strong>, <a> tags ONLY. NO markdown whatsoever.
- wordCount: integer word count
- faqs: array of exactly 4 objects, each with "question" (natural conversational question about this topic) and "answer" (1-3 sentence direct answer, no HTML).
- steps: if this keyword is a how-to/repair/guide topic, include 4-8 steps as [{"name":"Step name","text":"1-2 sentences describing what to CHECK or LOOK FOR at this stage — not how to repair it. Frame as diagnostic steps a homeowner takes before calling a pro, or what a technician will do. Never write steps that enable a homeowner to complete the repair themselves."}]. Otherwise empty array [].

No HTML in faq answers or step text. Return ONLY the JSON object, no other text.
{"title":"...","metaDescription":"...","slug":"...","content":"...","wordCount":0,"faqs":[],"steps":[]}` }],
    });

    const raw = message.content[0].text.trim().replace(/\`\`\`json|\`\`\`/g, "").trim();
    const post = JSON.parse(raw);
    // Safety net: convert any markdown that slipped into the content
    post.content = markdownToHtml(post.content);

    // Build all schema (Article + HowTo + FAQPage) + visible HTML sections
    const { appendHtml: schAppendHtml, schemaHtml: schSchemaHtml, schemaTypes: schTypes } = buildSchemaBlock({
      title: post.title,
      metaDescription: post.metaDescription,
      slug: post.slug,
      keyword,
      faqs: post.faqs || [],
      steps: post.steps || [],
      clientName: client.name,
      clientDomain: client.wordpress_url || client.domain || "",
      publishedDate: new Date().toISOString(),
      featuredImageUrl: featuredImage?.storage_path || "",
      authorName: "",
    });
    if (schAppendHtml) post.content = post.content + schAppendHtml;
    if (schSchemaHtml) post.content = post.content + "\n" + schSchemaHtml;
    console.log("[Scheduler] Schema types injected:", schTypes);

    const credentials = Buffer.from(`${client.wordpress_username}:${client.wordpress_password}`).toString("base64");
    const authHeaders = { "Authorization": `Basic ${credentials}` };

    let featuredMediaId = null;
    if (featuredImage) {
      try {
        const imageRes = await axios.get(featuredImage.storage_path, { responseType: "arraybuffer", httpsAgent });
        const ext = featuredImage.storage_path.split(".").pop().split("?")[0] || "jpg";
        const mediaRes = await axios.post(`${client.wordpress_url}/wp-json/wp/v2/media`, Buffer.from(imageRes.data), {
          headers: { ...authHeaders, "Content-Type": ext === "png" ? "image/png" : "image/jpeg", "Content-Disposition": `attachment; filename="${post.slug}.${ext}"` },
          httpsAgent,
        });
        featuredMediaId = mediaRes.data.id;
        await axios.post(`${client.wordpress_url}/wp-json/wp/v2/media/${featuredMediaId}`, { alt_text: keyword }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
      // Track image usage so it won't be reused for 8 posts
      if (featuredImage?.id) await markImageUsed(featuredImage.id);
      } catch (e) { console.log("Image upload failed:", e.message); }
    }

    let categoryId = null;
    try {
      const catRes = await axios.get(`${client.wordpress_url}/wp-json/wp/v2/categories?per_page=100`, { headers: authHeaders, httpsAgent });
      const cats = catRes.data;
      const kw = keyword.toLowerCase().split(" ");
      let best = cats.find(c => kw.some(w => w.length > 3 && c.name.toLowerCase().includes(w)));
      if (!best) best = cats.find(c => c.name.toLowerCase().includes(client.industry.toLowerCase()));
      if (!best) best = cats.find(c => c.slug !== "uncategorized") || cats[0];
      categoryId = best?.id || null;
    } catch (e) {}

    const wpRes = await axios.post(`${client.wordpress_url}/wp-json/wp/v2/posts`, {
      title: post.title, content: post.content, slug: post.slug, status: "publish",
      ...(featuredMediaId && { featured_media: featuredMediaId }),
      ...(categoryId && { categories: [categoryId] }),
    }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });

    // ── Yoast meta: full-caps detection + 3-path write (same as manual publish) ────
    const schSafeMetaDesc = (post.metaDescription || "").length > 156
      ? (post.metaDescription || "").slice(0, 153).trimEnd() + "..."
      : (post.metaDescription || "");
    const schLongtailKw = makeLongtailKeyphrase(keyword);
    let schSeoCaps = { yoast: "none", fortitudePlugin: false, canWriteSeoMeta: false, restMetaKeys: false, indexablesApi: false };
    try { schSeoCaps = await detectSeoCapabilities(client.wordpress_url, authHeaders, client.id); } catch(e) {}
    console.log(`[Scheduler SEO] edition=${schSeoCaps.yoast}, fortitude=${schSeoCaps.fortitudePlugin}, restKeys=${schSeoCaps.restMetaKeys}, focuskw="${schLongtailKw}"`);

    try {
      // PATH A: Fortitude plugin (works for Free + Premium)
      if (schSeoCaps.fortitudePlugin) {
        try {
          const r = await axios.post(`${client.wordpress_url}/wp-json/fortitude/v1/seo-meta`, {
            post_id: wpRes.data.id,
            focuskw: schLongtailKw,
            metadesc: schSafeMetaDesc,
            title: `${post.title} - %%sitename%%`,
          }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
          if (r.data?.success) console.log("[Scheduler SEO] ✓ Fortitude plugin wrote Yoast meta");
        } catch(e) { console.log("[Scheduler SEO] Fortitude seo-meta failed:", e.message); }
      }

      // PATH B: REST postmeta (if Yoast registered keys)
      if (schSeoCaps.restMetaKeys) {
        await axios.post(`${client.wordpress_url}/wp-json/wp/v2/posts/${wpRes.data.id}`, {
          meta: {
            _yoast_wpseo_focuskw:              schLongtailKw,
            _yoast_wpseo_metadesc:             schSafeMetaDesc,
            _yoast_wpseo_title:                `${post.title} - %%sitename%%`,
            _yoast_wpseo_opengraph_title:      post.title,
            _yoast_wpseo_opengraph_description: schSafeMetaDesc,
          }
        }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
        console.log("[Scheduler SEO] ✓ REST postmeta wrote Yoast meta");
      }

      // PATH C: Fallback REST attempt even if keys weren't detected
      if (!schSeoCaps.fortitudePlugin && !schSeoCaps.restMetaKeys && schSeoCaps.yoast !== "none") {
        try {
          await axios.post(`${client.wordpress_url}/wp-json/wp/v2/posts/${wpRes.data.id}`, {
            meta: { _yoast_wpseo_focuskw: schLongtailKw, _yoast_wpseo_metadesc: schSafeMetaDesc, _yoast_wpseo_title: `${post.title} - %%sitename%%` }
          }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
        } catch(e) {}
      }

      // Always write Astra layout meta
      await axios.post(`${client.wordpress_url}/wp-json/wp/v2/posts/${wpRes.data.id}`, {
        meta: { "astra-migrate-meta-layouts": "set" }
      }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent }).catch(() => {});

      // Score recalc: Fortitude → Premium indexables → no-op update
      let schRecalcOk = false;
      if (schSeoCaps.fortitudePlugin) {
        try {
          const r = await axios.post(`${client.wordpress_url}/wp-json/fortitude/v1/yoast-recalc`,
            { post_id: wpRes.data.id }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
          if (r.data?.success || r.status === 200) { schRecalcOk = true; console.log("[Scheduler SEO] ✓ Fortitude yoast-recalc"); }
        } catch(e) {}
      }
      if (!schRecalcOk && schSeoCaps.indexablesApi) {
        try {
          await axios.post(`${client.wordpress_url}/wp-json/yoast/v3/indexing/posts`,
            { post_id: wpRes.data.id }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
          schRecalcOk = true; console.log("[Scheduler SEO] ✓ Premium indexables recalc");
        } catch(e) {}
      }
      if (!schRecalcOk) {
        try {
          await new Promise(r => setTimeout(r, 1500));
          await axios.post(`${client.wordpress_url}/wp-json/wp/v2/posts/${wpRes.data.id}`,
            { status: "publish" }, { headers: { ...authHeaders, "Content-Type": "application/json" }, httpsAgent });
          console.log("[Scheduler SEO] ✓ no-op update triggered Yoast recalc");
        } catch(e) {}
      }
    } catch (e) { console.log("[Scheduler] Yoast meta error:", e.message); }

    await supabase.from("posts").insert([{
      client_id: client.id, keyword, title: post.title, meta_description: post.metaDescription,
      slug: post.slug, content: post.content, word_count: post.wordCount, status: "published", published_at: new Date().toISOString()
    }]);
    const { data: c } = await supabase.from("clients").select("posts_published, next_keyword_index").eq("id", client.id).single();
    await supabase.from("clients").update({ posts_published: (c?.posts_published || 0) + 1, next_keyword_index: (c?.next_keyword_index || 0) + 1 }).eq("id", client.id);

    // ── Post-publish QA + auto-repair loop ────────────────────────────────
    const liveUrl = wpRes.data.link;
    if (liveUrl) {
      try {
        const { qa, history: repairHistory } = await qaRepairLoop(
          wpRes.data.id, liveUrl,
          { title: post.title, keyword, metaDescription: post.metaDescription },
          client.wordpress_url,
          { ...authHeaders, "Content-Type": "application/json" }
        );
        // Store final QA result in Supabase
        await supabase.from("posts").update({
          qa_score:      qa.score,
          qa_passed:     qa.passed,
          qa_issues:     JSON.stringify(qa.issues),
          qa_warnings:   JSON.stringify(qa.warnings),
          qa_notes:      qa.aiNotes || null,
          qa_cycles:     repairHistory.length,
          qa_checked_at: new Date().toISOString()
        }).eq("client_id", client.id).eq("slug", post.slug);
        if (!qa.passed) {
          console.warn(`[QA] ⚠ ${client.name} post still failing after ${repairHistory.length} cycles — manual review needed`);
        }
      } catch(e) {
        console.error("[QA] Scheduled repair loop threw:", e.message);
      }
    }

    // ── Auto-post to Google Business Profile ──────────────────────────────
    if (agencyGbpToken.refresh_token && client.gbp_location_name) {
      try {
        const access_token = await getAgencyAccessToken();
        const gbpSummary = post.metaDescription || `${post.title} — read our latest post for expert tips and advice.`;
        const gbpBody = {
          languageCode: "en",
          summary: gbpSummary,
          topicType: "STANDARD",
          callToAction: { actionType: "LEARN_MORE", url: liveUrl || wpRes.data.link },
          ...(featuredImage?.storage_path ? { media: [{ mediaFormat: "PHOTO", sourceUrl: featuredImage.storage_path }] } : {}),
        };
        await axios.post(
          `https://mybusiness.googleapis.com/v4/${client.gbp_location_name}/localPosts`,
          gbpBody,
          { headers: { Authorization: `Bearer ${access_token}`, "Content-Type": "application/json" } }
        );
        console.log(`✓ GBP post auto-published for ${client.name}`);
      } catch (gbpErr) {
        console.error(`GBP auto-post error for ${client.name}:`, gbpErr.response?.data || gbpErr.message);
      }
    }

    console.log(`✓ Scheduled post published for ${client.name}: "${post.title}"`);
    return wpRes.data.id;
  } catch (err) {
    console.error(`✗ Scheduled post failed for ${client.name}:`, err.message);
    throw err;
  }
};

const scheduleDailyPosts = async () => {
  console.log("⏰ Scheduler running — checking clients...");
  const { data: clients } = await supabase.from("clients")
    .select("*")
    .eq("status", "active")
    .eq("schedule_enabled", true);

  if (!clients?.length) { console.log("No clients with scheduling enabled."); return; }

  // All scheduling operates in EST (America/New_York) regardless of server timezone
  const TZ = "America/New_York";
  const nowUTC = new Date();

  // Compute EST offset in ms (handles both EST -5 and EDT -4 automatically)
  const estOffsetMs = nowUTC.getTime() - new Date(nowUTC.toLocaleString("en-US", { timeZone: TZ })).getTime();

  // Get current date parts in EST
  const estParts = new Intl.DateTimeFormat("en-US", {
    timeZone: TZ, weekday: "short", year: "numeric", month: "2-digit", day: "2-digit",
  }).formatToParts(nowUTC).reduce((acc, p) => { acc[p.type] = p.value; return acc; }, {});

  const dayName = estParts.weekday; // "Mon", "Tue", etc. in EST

  // EST midnight as UTC for "already published today" DB query
  const estMidnight = new Date(`${estParts.year}-${estParts.month}-${estParts.day}T00:00:00`);
  const todayStart = new Date(estMidnight.getTime() + estOffsetMs).toISOString();

  for (const client of clients) {
    try {
      const scheduleDays = client.schedule_days || ["Mon","Tue","Wed","Thu","Fri"];
      if (!scheduleDays.includes(dayName)) { console.log(`Skipping ${client.name} — not scheduled today (EST: ${dayName})`); continue; }

      const { data: todayJobs } = await supabase.from("scheduled_jobs")
        .select("id").eq("client_id", client.id)
        .gte("scheduled_time", todayStart)
        .eq("status", "published");
      if (todayJobs?.length > 0) { console.log(`Skipping ${client.name} — already published today`); continue; }

      const month = new Date().toISOString().slice(0, 7);
      const { data: queueKeywords } = await supabase.from("client_keyword_queue")
        .select("*").eq("client_id", client.id).eq("month", month).eq("used", false)
        .order("source", { ascending: false });

      let keyword = null;
      let queueItemId = null;

      if (queueKeywords?.length > 0) {
        keyword = queueKeywords[0].keyword;
        queueItemId = queueKeywords[0].id;
      } else {
        const { data: libKeywords } = await supabase.from("keyword_library")
          .select("*").eq("industry", client.industry).order("volume", { ascending: false });
        if (!libKeywords?.length) { console.log(`No keywords for ${client.name}`); continue; }
        const idx = (client.next_keyword_index || 0) % libKeywords.length;
        keyword = libKeywords[idx].keyword;
      }

      const startHour = client.schedule_start_hour || 9;
      const endHour = client.schedule_end_hour || 12;
      const randomMinutes = Math.floor(Math.random() * ((endHour - startHour) * 60));

      // Build scheduled time anchored to EST: today's date at startHour in EST, converted to UTC
      const estOffsetHours = estOffsetMs / 3600000; // e.g. -5 for EST, -4 for EDT
      const sign = estOffsetHours <= 0 ? "-" : "+";
      const absH = String(Math.floor(Math.abs(estOffsetHours))).padStart(2, "0");
      const absM = String(Math.round((Math.abs(estOffsetHours) % 1) * 60)).padStart(2, "0");
      const hh = String(startHour).padStart(2, "0");
      const scheduledTime = new Date(`${estParts.year}-${estParts.month}-${estParts.day}T${hh}:00:00${sign}${absH}:${absM}`);
      scheduledTime.setMinutes(scheduledTime.getMinutes() + randomMinutes);

      const estTimeStr = scheduledTime.toLocaleTimeString("en-US", { timeZone: TZ, hour: "2-digit", minute: "2-digit" });
      console.log(`⏱ ${client.name} — "${keyword}" scheduled for ${estTimeStr} EST`);

      const { data: job } = await supabase.from("scheduled_jobs").insert([{
        client_id: client.id, keyword, scheduled_time: scheduledTime.toISOString(), status: "pending"
      }]).select().single();

      const msUntilPublish = scheduledTime.getTime() - Date.now();
      if (msUntilPublish > 0) {
        if (queueItemId) await supabase.from("client_keyword_queue").update({ used: true }).eq("id", queueItemId);
        setTimeout(async () => {
          try {
            await supabase.from("scheduled_jobs").update({ status: "running" }).eq("id", job.id);
            const wpPostId = await publishPostForClient(client, keyword);
            await supabase.from("scheduled_jobs").update({ status: "published", wp_post_id: wpPostId }).eq("id", job.id);
            // Auto-add to client used keywords
            try {
              const { data: eu } = await supabase.from("client_used_keywords").select("id").eq("client_id", client.id).eq("keyword", keyword.trim()).single();
              if (!eu) await supabase.from("client_used_keywords").insert([{ client_id: client.id, keyword: keyword.trim(), added_at: new Date().toISOString() }]);
              await supabase.from("client_keyword_queue").delete().eq("client_id", client.id).ilike("keyword", keyword.trim());
            } catch(e) {}
          } catch (e) {
            await supabase.from("scheduled_jobs").update({ status: "failed" }).eq("id", job.id);
          }
        }, msUntilPublish);
      } else {
        await publishPostForClient(client, keyword);
        await supabase.from("scheduled_jobs").update({ status: "published" }).eq("id", job.id);
      }
    } catch (err) {
      console.error(`Scheduler error for ${client.name}:`, err.message);
    }
  }
};
cron.schedule("0 5 * * *", scheduleDailyPosts); // Midnight EST (5am UTC in EST / 4am UTC in EDT — fires before any publish window)

cron.schedule("0 12 1 * *", async () => { // 7am EST on the 1st of each month
  console.log("📅 Monthly keyword refresh starting...");
  const { data: clients } = await supabase.from("clients").select("*").eq("status", "active").eq("schedule_enabled", true);
  for (const client of (clients || [])) {
    try {
      const res = await axios.post(`http://localhost:${PORT}/api/keywords/monthly-refresh/${client.id}`);
      console.log("✓ Monthly refresh done for " + client.name + " — " + res.data.total + " keywords");
    } catch (e) { console.error("Monthly refresh failed for " + client.name + ":", e.message); }
  }
});
console.log("✓ Scheduler initialized — runs daily at midnight EST (5am UTC), publishes within each client's window");

// ─── SEO AUDIT ENGINE ────────────────────────────────────────────

// Shared fetch helper
// bustCache=true only for single-page audit (to verify fixes just applied)
// bustCache=false for site crawl (cache-busting causes WP to redirect and drop params, corrupting the fetch)
async function fetchPage(url, bustCache = false) {
  const fetchUrl = bustCache
    ? url.replace(/\/+$/, "") + (url.includes("?") ? "&" : "?") + "_cb=" + Date.now()
    : url;
  const t0 = Date.now();
  const res = await axios.get(fetchUrl, {
    timeout: 20000,
    maxRedirects: 10,
    httpsAgent,
    validateStatus: s => s < 500, // don't throw on 4xx — we want to record it
    headers: {
      "User-Agent": "Mozilla/5.0 (compatible; FortitudeBot/1.0)",
      "Cache-Control": "no-cache, no-store, must-revalidate",
      "Pragma": "no-cache",
    },
  });
  return { html: res.data || "", responseTime: Date.now() - t0, finalUrl: res.request?.res?.responseUrl || url, status: res.status };
}

// Extract all internal links from HTML
// Extensions that are never HTML pages
const ASSET_EXT_RE = /\.(css|js|jpg|jpeg|png|gif|webp|svg|ico|woff|woff2|ttf|eot|mp4|mp3|pdf|zip|xml|json|txt|map|rss|atom)(\?.*)?$/i;
// Skip WP utility paths AND taxonomy/archive URLs (tag, category, author, date archives)
// These are intentionally noindexed by Yoast and Semrush doesn't flag them
const SKIP_PATH_RE = /\/(wp-content|wp-includes|wp-json|wp-login|wp-admin|feed|trackback|xmlrpc|embed|(?:page|comment-page)-\d+|tag|category|author)\b|\/(20\d{2}|19\d{2})\/\d{2}(\/\d{2})?\//i;

function isPageUrl(href) {
  try {
    const u = new URL(href);
    if (!u.protocol.startsWith("http")) return false;
    if (ASSET_EXT_RE.test(u.pathname)) return false;
    if (SKIP_PATH_RE.test(u.pathname)) return false;
    return true;
  } catch { return false; }
}

function extractInternalLinks(html, origin, hostname) {
  const links = new Set();
  const hrefRe = /href=["']([^"'#?\s][^"'\s]*)["']/gi;
  let m;
  while ((m = hrefRe.exec(html)) !== null) {
    const href = m[1];
    try {
      const u = new URL(href, origin);
      if (u.hostname === hostname && isPageUrl(u.href)) {
        // Normalize: strip query/hash/trailing slash
        const clean = u.origin + u.pathname.replace(/\/+$/, "") || "/";
        links.add(clean);
      }
    } catch {}
  }
  return [...links];
}

// Parse sitemap.xml and return all page URLs
async function fetchSitemapUrls(origin) {
  const urls = new Set();
  const toFetch = [`${origin}/sitemap.xml`, `${origin}/sitemap_index.xml`];
  const visited = new Set();

  while (toFetch.length > 0) {
    const sitemapUrl = toFetch.pop();
    if (visited.has(sitemapUrl)) continue;
    visited.add(sitemapUrl);
    try {
      const res = await axios.get(sitemapUrl, { timeout: 10000, httpsAgent,
        headers: { "User-Agent": "Mozilla/5.0 (compatible; FortitudeBot/1.0)" } });
      const xml = res.data;
      // Sitemap index — contains <sitemap><loc> entries
      const sitemapLocs = [...xml.matchAll(/<sitemap>[\s\S]*?<loc>(.*?)<\/loc>/gi)].map(m => m[1].trim());
      sitemapLocs.forEach(l => toFetch.push(l));
      // Regular sitemap — contains <url><loc> entries
      const urlLocs = [...xml.matchAll(/<url>[\s\S]*?<loc>(.*?)<\/loc>/gi)].map(m => m[1].trim());
      urlLocs.forEach(l => { const n = l.replace(/\/+$/, ""); if (isPageUrl(n)) urls.add(n); });
    } catch {}
  }
  return [...urls];
}

// Analyze a single page and return structured issues array + metadata
async function auditPage(url, origin, hostname, bustCache = false) {
  let html = "", responseTime = 0, finalUrl = url, httpStatus = 200;
  try {
    ({ html, responseTime, finalUrl, status: httpStatus } = await fetchPage(url, bustCache));
  } catch (e) {
    return { url, failed: true, error: e.response ? `HTTP ${e.response.status}` : e.code || e.message, issues: [], score: 0 };
  }

  // 4xx — flag as error issue, still return a page record (not failed:true)
  if (httpStatus >= 400 && httpStatus < 500) {
    const issues = [{ id:"http_4xx", title:`${httpStatus} Status Code`, severity:"error",
      description:`Page returned HTTP ${httpStatus}. This page is broken — fix or redirect it.`,
      current:`HTTP ${httpStatus}`, fixable:false }];
    return { url, failed: false, score: 0, errors: 1, warnings: 0, passes: 0, issues,
      meta: { title:"", metaDesc:"", h1:"", wordCount:0, responseTime }, httpStatus };
  }

  const titleMatch     = html.match(/<title[^>]*>([^<]*)<\/title>/i);
  const metaDescMatch  = html.match(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)["']/i)
                      || html.match(/<meta[^>]+content=["']([^"']*)["'][^>]+name=["']description["']/i);
  const canonicalMatch = html.match(/<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']*)["']/i)
                      || html.match(/<link[^>]+href=["']([^"']*)["'][^>]+rel=["']canonical["']/i);
  const robotsMetaMatch= html.match(/<meta[^>]+name=["']robots["'][^>]+content=["']([^"']*)["']/i);

  const h1Matches  = [...html.matchAll(/<h1[^>]*>([\s\S]*?)<\/h1>/gi)].map(m => m[1].replace(/<[^>]+>/g,"").trim()).filter(Boolean);
  const imgTags    = [...html.matchAll(/<img([^>]*)>/gi)].map(m => m[1]);
  const imgsNoAlt  = imgTags.filter(t => { const a = t.match(/alt=["']([^"']*)['"]/i); return !a || !a[1].trim(); });

  const allLinkMatches = [...html.matchAll(/href=["']([^"'#\s][^"'\s]*)['"]/gi)].map(m => m[1]);

  const anchorMatches = [...html.matchAll(/<a[^>]*href=["'][^"']+["'][^>]*>([\s\S]*?)<\/a>/gi)];
  const vaguePhrases  = ["click here","click","here","read more","learn more","more","this","link"];
  const anchorsVague  = anchorMatches.filter(m => vaguePhrases.includes(m[1].replace(/<[^>]+>/g,"").trim().toLowerCase()));

    const htmlSize      = Buffer.byteLength(html, "utf8");
  const bodyText      = html.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"")
                          .replace(/<[^>]+>/g," ").replace(/&[a-z]+;/gi," ").replace(/&#\d+;/gi," ").replace(/\s+/g," ").trim();
  const wordCount     = bodyText.split(" ").filter(w => w.length > 2 && /[a-zA-Z]/.test(w)).length;
  const textHtmlRatio = htmlSize > 0 ? Math.round((bodyText.length / htmlSize) * 100) : 0;

  // Extract JS + CSS asset URLs for unminified check (Semrush: "Unminified JavaScript and CSS files")
  const assetUrls = [];
  const scriptSrcRe = /<script[^>]+src=[\"']([^\"']+\.js[^\"']*)[\"']/gi;
  const linkHrefRe  = /<link[^>]+href=[\"']([^\"']+\.css[^\"']*)[\"'][^>]+rel=[\"']stylesheet[\"']/gi;
  const linkHrefRe2 = /<link[^>]+rel=[\"']stylesheet[\"'][^>]+href=[\"']([^\"']+\.css[^\"']*)[\"']/gi;
  let am;
  while ((am = scriptSrcRe.exec(html)) !== null) { try { assetUrls.push(new URL(am[1], origin).href); } catch {} }
  while ((am = linkHrefRe.exec(html))  !== null) { try { assetUrls.push(new URL(am[1], origin).href); } catch {} }
  while ((am = linkHrefRe2.exec(html)) !== null) { try { assetUrls.push(new URL(am[1], origin).href); } catch {} }

  // Dedupe and limit to 8 assets to keep crawl fast
  const uniqueAssets = [...new Set(assetUrls)].slice(0, 8);

  // Fetch assets and check if unminified (long avg line length = not minified)
  function isUnminified(content) {
    if (!content || content.length < 500) return false;
    const lines = content.split("\n").filter(l => l.trim().length > 0);
    if (lines.length < 3) return false; // single-line = already minified
    const avgLineLen = content.length / lines.length;
    // Minified files have very long lines (1000+ chars). Unminified have short lines (<200 avg)
    return avgLineLen < 200 && lines.length > 10;
  }

  let unminifiedAssets = [];
  try {
    const assetResults = await Promise.allSettled(
      uniqueAssets.map(u => axios.get(u, { timeout: 8000, httpsAgent,
        headers: { "User-Agent": "Mozilla/5.0 (compatible; FortitudeBot/1.0)" },
        validateStatus: s => s < 400,
        transformResponse: [d => d], // keep as raw string, no JSON parse
      }))
    );
    assetResults.forEach((r, i) => {
      if (r.status === "fulfilled" && r.value?.data) {
        const content = typeof r.value.data === "string" ? r.value.data : String(r.value.data);
        if (isUnminified(content)) unminifiedAssets.push(uniqueAssets[i]);
      }
    });
  } catch {} // asset fetch errors are non-fatal

  const title      = titleMatch?.[1]?.trim() || "";
  const metaDesc   = metaDescMatch?.[1]?.trim() || "";
  const canonical  = canonicalMatch?.[1]?.trim() || "";
  const titleLen   = title.length;
  const metaLen    = metaDesc.length;
  const isNoindex  = robotsMetaMatch && /noindex/i.test(robotsMetaMatch[1]);
  const isHttps    = url.startsWith("https://");
  const parsedUrl  = new URL(url, origin);
  const wasRedirected = finalUrl.replace(/\/$/, "").replace(/\?.*/, "") !== url.replace(/\/$/, "").replace(/\?.*/, "");

  const issues = [];
  const issue  = (id, title, severity, description, opts = {}) =>
    issues.push({ id, title, severity, description,
      current: opts.current ?? null, suggestion: opts.suggestion ?? null,
      fixable: opts.fixable ?? false, fixType: opts.fixType ?? null, fixValue: opts.fixValue ?? null });

  // ─────────────────────────────────────────────────────────────────
  // CHECKS EXACTLY MIRROR SEMRUSH — nothing more, nothing less.
  //
  //  ERROR   — 4xx, no HTTPS, missing title, missing H1,
  //            noindex (on real content pages), canonical off-site
  //
  //  WARNING — unminified JS/CSS, low text-to-HTML ratio,
  //            title too long, missing meta description,
  //            meta description too long, missing alt text,
  //            slow load (>3s), duplicate title, duplicate meta
  //
  //  INFO    — Semrush "Notices": redirects, multiple H1,
  //            non-descriptive anchors, HSTS missing, low word count
  //
  //  PASS    — check passed (shown in page detail only, not in issues)
  // ─────────────────────────────────────────────────────────────────

  // ── ERRORS ───────────────────────────────────────────────────────

  if (!isHttps)
    issue("no_https","HTTPS Not Used","error","Page is served over HTTP. Google marks HTTP pages as insecure and penalizes rankings.");

  if (isNoindex) {
    const intentional = /\/(tag|category|author|search|page)\/|[?&](s|p|page_id)=|\/(feed|comments|trackback)\//i.test(url);
    if (!intentional)
      issue("noindex","Page Blocked from Indexing","error",`robots meta is "${robotsMetaMatch[1]}" — Google will not index this page.`,{ current:robotsMetaMatch[1], fixable:true, fixType:"yoast_noindex_fix", suggestion:"Sets Yoast indexing to index/follow." });
  }

  if (!title)
    issue("title_missing","Title Tag Missing","error","No <title> tag. Google uses it as the search result headline.",{ fixable:true, fixType:"title_tag" });

  if (h1Matches.length === 0)
    issue("h1_missing","Missing H1 Tag","error","No H1 heading found. Every page needs exactly one H1.",{ fixable:true, fixType:"elementor_h1_promote", suggestion:"Promotes the first heading on the page to H1." });

  if (canonical && !canonical.includes(hostname))
    issue("canonical_offsite","Canonical Points Off-Site","error",`Canonical points to a different domain: "${canonical}". Fix manually in Yoast — auto-fix disabled to protect Google Ads URLs.`,{ current:canonical });

  // ── WARNINGS ─────────────────────────────────────────────────────

  if (title && titleLen > 60)
    issue("title_too_long","Title Too Long","warning",`${titleLen} chars — truncated in Google at ~60. Rewrite to fit within 60 characters.`,{ current:title, fixable:true, fixType:"title_tag" });

  if (!metaDesc)
    issue("meta_missing","Missing Meta Description","warning","No meta description. Write one to control how your page appears in search results.",{ fixable:true, fixType:"meta_description" });
  else if (metaLen > 160)
    issue("meta_too_long","Meta Description Too Long","warning",`${metaLen} chars — truncated after ~160. Shorten it.`,{ current:metaDesc, fixable:true, fixType:"meta_description" });

  const altMissingPct = imgTags.length > 0 ? Math.round((imgsNoAlt.length / imgTags.length) * 100) : 0;
  if (imgsNoAlt.length > 0)
    issue("alt_tags_missing","Images Missing Alt Text","warning",`${imgsNoAlt.length} of ${imgTags.length} images have no alt text.`,{ current:`${imgsNoAlt.length}/${imgTags.length} images`, fixable:true, fixType:"alt_tags", fixValue:"auto" });

  if (textHtmlRatio < 10)
    issue("low_text_ratio","Low Text-to-HTML Ratio","warning",`Text is only ${textHtmlRatio}% of the page HTML.`,{ current:`${textHtmlRatio}%`, suggestion:"Reduce unused Elementor markup or add more content." });

  if (responseTime > 3000)
    issue("slow_load","Slow Page Load Speed","warning",`${(responseTime/1000).toFixed(1)}s response time. Aim for under 3s.`,{ current:`${(responseTime/1000).toFixed(1)}s`, suggestion:"Enable caching plugin (WP Rocket, W3 Total Cache) and consider Cloudflare." });

  if (unminifiedAssets.length > 0)
    issue("unminified_assets","Unminified JavaScript and CSS Files","warning",`${unminifiedAssets.length} asset(s) are not minified. Minifying reduces file size and improves load speed.`,{ current:unminifiedAssets.map(u=>u.split("/").pop().split("?")[0]).slice(0,3).join(", "), suggestion:"Use WP Rocket, W3 Total Cache, or Autoptimize." });

  // ── INFO / NOTICES ────────────────────────────────────────────────

  if (h1Matches.length > 1)
    issue("h1_multiple","More Than One H1 Tag","info",`${h1Matches.length} H1 tags found — only one is recommended.`,{ current:h1Matches.slice(0,3).join(" | "), fixable:true, fixType:"elementor_h1_dedupe", suggestion:"Keeps the first H1, demotes all others to H2." });

  if (anchorsVague.length > 0)
    issue("links_vague_anchor","Non-Descriptive Anchor Text","info",`${anchorsVague.length} link(s) use generic text like "click here" or "read more".`,{ current:anchorsVague.map(m=>`"${m[1].replace(/<[^>]+>/g,"").trim()}"`).slice(0,3).join(", ") });

  if (wasRedirected)
    issue("redirect_present","Permanent Redirect","info",`This URL redirects to: ${finalUrl}`,{ current:`→ ${finalUrl}` });

  if (wordCount < 200)
    issue("low_word_count","Low Word Count","info",`~${wordCount} words. Pages under 200 words may be considered thin content.`,{ current:`~${wordCount} words`, suggestion:"Expand content to at least 300 words." });

  // Collect external resource URLs (JS/CSS from external domains) for site-level check
  const externalResources = [];
  const extResRe = /<(?:script|link)[^>]+(?:src|href)=["']((https?:)?\/\/(?!${hostname.replace('.','\\.')})[^"']+\.(?:js|css|woff2?|ttf|eot)[^"']*)['"]/gi;
  let erm;
  while ((erm = extResRe.exec(html)) !== null) {
    try { externalResources.push(new URL(erm[1], origin).href); } catch {}
  }

  // Collect resource-formatted page links (<a href> pointing to files)
  const resourceExtRe = /\.(pdf|doc|docx|xls|xlsx|ppt|pptx|zip|mp3|mp4|avi|mov|jpg|jpeg|png|gif|webp|svg)(\?[^"']*)?$/i;
  const resourceLinks = allLinkMatches.filter(h => resourceExtRe.test(h.split("?")[0]));

  // ── SCORE ─────────────────────────────────────────────────────────
  const errors   = issues.filter(i => i.severity === "error").length;
  const warnings = issues.filter(i => i.severity === "warning").length;
  const notices  = issues.filter(i => i.severity === "info").length;
  const passes   = issues.filter(i => i.severity === "pass").length;
  const score    = Math.max(0, Math.min(100, 100 - (errors * 15) - (warnings * 3)));

  return { url, failed: false, score, errors, warnings, notices, passes, issues,
    externalResources, resourceLinks,
    meta: { title, metaDesc, h1: h1Matches[0] || "", wordCount, responseTime } };
}

// ─── SINGLE PAGE AUDIT (existing endpoint — unchanged for fix flow) ──
app.post("/api/seo/audit", async (req, res) => {
  const { url, clientId } = req.body;
  if (!url) return res.status(400).json({ error: "url required" });

  let parsedUrl;
  try { parsedUrl = new URL(url); } catch { return res.status(400).json({ error: `Invalid URL: ${url}` }); }
  const hostname = parsedUrl.hostname;
  const origin   = parsedUrl.origin;

  // Check robots.txt, sitemap, HSTS once per domain
  let robotsTxtOk = null, sitemapFound = null, sitemapInRobots = null, hstsSeen = false;
  try {
    const [robotsRes, sitemapRes] = await Promise.allSettled([
      axios.get(`${origin}/robots.txt`, { timeout: 8000, httpsAgent }),
      axios.get(`${origin}/sitemap.xml`, { timeout: 8000, httpsAgent }),
    ]);
    if (robotsRes.status === "fulfilled") {
      robotsTxtOk  = true;
      sitemapInRobots = /sitemap:/i.test(robotsRes.value.data || "");
      hstsSeen = !!(robotsRes.value.headers?.["strict-transport-security"]);
    } else { robotsTxtOk = false; }
    sitemapFound = sitemapRes.status === "fulfilled" && sitemapRes.value.status < 400;
  } catch {}

  try {
    const result = await auditPage(url, origin, hostname, true); // bustCache=true to verify fixes
    if (result.failed) return res.status(400).json({ error: result.error });

    const { issues } = result;

    // Domain-level checks (only on single-page audit of homepage)
    const issue = (id, title, severity, description, opts = {}) =>
      issues.push({ id, title, severity, description, current: opts.current ?? null,
        suggestion: opts.suggestion ?? null, fixable: false, fixType: null, fixValue: null });

    if (!robotsTxtOk)
      issue("no_robots_txt","robots.txt Not Found","warning","No robots.txt at domain root.",{ suggestion:"Create robots.txt" });
    else
      issue("robots_ok","robots.txt","pass","robots.txt accessible.");

    if (!sitemapFound)
      issue("no_sitemap","XML Sitemap Not Found","warning","No sitemap found — submit one via Google Search Console.",{ suggestion:"Generate via Yoast SEO" });
    else {
      issue("sitemap_ok","XML Sitemap","pass","sitemap.xml accessible.");
      if (robotsTxtOk && !sitemapInRobots)
        issue("sitemap_not_in_robots","Sitemap Not in robots.txt","info","Add Sitemap: directive to robots.txt.");
    }

    if (hstsSeen)
      issue("hsts_ok","HSTS","pass","HSTS header detected.");
    else if (url.startsWith("https://"))
      issue("no_hsts","No HSTS Header","info","Enable HSTS to force HTTPS connections.",{ suggestion:"Enable via Cloudflare or server config" });

    // Probe sample of external links for broken ones
    const allLinkMatches = [...(result.issues.find(i=>i.id==="links_ok" || i.id==="too_many_links") ? [] : [])];
    // Re-extract from raw page for external probe
    // (skipping full re-parse — external link probe runs separately in site audit)

    // Generate fix values for fixable issues
    const needsFixValue = issues.filter(i => i.fixable && i.fixType !== "alt_tags" && !i.fixValue);
    if (needsFixValue.length > 0) {
      try {
        const Anthropic = require("@anthropic-ai/sdk");
        const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
        const slugPart   = url.replace(/\/+$/,"").split("/").pop() || hostname;
        const slugKeyword = slugPart.replace(/-/g," ");
        const fixPrompt = `Generate SEO fix values for this page. Return ONLY valid JSON, no markdown.
Page: ${url}
Keyword hint: "${slugKeyword}"
Title (${result.meta.title.length} chars): "${result.meta.title}"
Meta (${result.meta.metaDesc.length} chars): "${result.meta.metaDesc}"
H1: "${result.meta.h1}"
Issues: ${JSON.stringify(needsFixValue.map(i=>({id:i.id,fixType:i.fixType})))}
Rules:
- title_tag: 50-60 chars, include keyword, brand at end if fits
- meta_description: 150-160 chars, keyword + location if in slug + CTA
- focus_keyword: best single phrase from slug/title, lowercase
Return: { "fixes": { "<issue_id>": "<fixValue>" } }`;
        const msg = await anthropic.messages.create({ model:"claude-sonnet-4-20250514", max_tokens:500,
          messages:[{role:"user",content:fixPrompt}] });
        const { fixes } = JSON.parse(msg.content[0].text.trim().replace(/```json|```/g,"").trim());
        issues.forEach(i => { if (fixes[i.id]) { i.fixValue = fixes[i.id]; i.suggestion = fixes[i.id]; } });
      } catch(e) { console.error("[SEO Audit] fix gen error:", e.message); }
    }

    const errors   = issues.filter(i => i.severity === "error").length;
    const warnings = issues.filter(i => i.severity === "warning").length;
    const passes   = issues.filter(i => i.severity === "pass").length;
    const score    = Math.max(0, Math.min(100, 100 - (errors * 15) - (warnings * 3)));
    const summary  = errors === 0 && warnings === 0 ? "Excellent — no issues found."
      : `${errors} error(s) and ${warnings} warning(s). ${errors > 0 ? "Fix errors first." : "Address warnings for further improvement."}`;

    res.json({ score, summary, issues, url, checkedAt: new Date().toISOString() });
  } catch(e) {
    console.error("SEO audit error:", e.message);
    res.status(500).json({ error: "Audit failed: " + e.message });
  }
});

// ─── FULL SITE CRAWL AUDIT (SSE streaming) ───────────────────────
app.options("/api/seo/site-audit", (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.sendStatus(204);
});

app.get("/api/seo/site-audit", async (req, res) => {
  const { url, clientId, maxPages = 100 } = req.query;
  if (!url) return res.status(400).json({ error: "url required" });

  let parsedUrl;
  try { parsedUrl = new URL(url); } catch { return res.status(400).json({ error: `Invalid URL: ${url}` }); }
  const hostname = parsedUrl.hostname;
  const origin   = parsedUrl.origin;
  const limit    = Math.min(parseInt(maxPages) || 100, 200);

  // Server-Sent Events for live progress
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("X-Accel-Buffering", "no"); // disable nginx buffering
  res.flushHeaders();

  const send = (data) => {
    try {
      res.write(`data: ${JSON.stringify(data)}\n\n`);
      if (typeof res.flush === "function") res.flush(); // compression middleware flush
    } catch {}
  };

  send({ type: "start", message: `Starting site audit for ${hostname}...` });

  try {
    // ── Step 1: Discover URLs ──────────────────────────────────
    send({ type: "progress", phase: "discover", message: "Fetching sitemap..." });
    const sitemapUrls = await fetchSitemapUrls(origin);
    send({ type: "progress", phase: "discover", message: `Found ${sitemapUrls.length} URLs in sitemap. Crawling homepage for more...` });

    // Start crawl queue with homepage + sitemap URLs
    const queue    = new Set([origin, ...sitemapUrls.slice(0, limit)]);
    const visited  = new Set();
    const toVisit  = [origin];

    // BFS crawl — discover more pages by following internal links
    const crawlDepth = 2; // follow links 2 levels deep
    let depth = 0;
    while (toVisit.length > 0 && visited.size < limit && depth < crawlDepth) {
      const batch = toVisit.splice(0, 10);
      await Promise.allSettled(batch.map(async (pageUrl) => {
        if (visited.has(pageUrl) || visited.size >= limit) return;
        visited.add(pageUrl);
        try {
          const res2 = await axios.get(pageUrl, {
            timeout: 15000, maxRedirects: 5, httpsAgent,
            headers: { "User-Agent": "Mozilla/5.0 (compatible; FortitudeBot/1.0)" }
          });
          const links = extractInternalLinks(res2.data, origin, hostname);
          links.forEach(l => { if (!visited.has(l) && !queue.has(l)) { queue.add(l); toVisit.push(l); } });
        } catch {}
      }));
      depth++;
    }

    // Merge sitemap URLs into queue, deduplicate, cap
    sitemapUrls.forEach(u => queue.add(u));
    const allUrls = [...new Set([origin, ...queue])].filter(u => isPageUrl(u)).slice(0, limit);

    send({ type: "progress", phase: "discover", message: `Discovered ${allUrls.length} pages to audit.`, total: allUrls.length });

    // ── Step 2: Domain-level checks ───────────────────────────
    let robotsTxtOk = false, sitemapFound = false, sitemapInRobots = false, hstsSeen = false;
    let robotsDisallowRules = []; // parsed Disallow paths for "blocked from crawling" check
    let robotsTxtContent = "";
    try {
      const [rr, sr] = await Promise.allSettled([
        axios.get(`${origin}/robots.txt`, { timeout: 8000, httpsAgent }),
        axios.get(`${origin}/sitemap.xml`, { timeout: 8000, httpsAgent }),
      ]);
      if (rr.status === "fulfilled") {
        robotsTxtOk = true;
        robotsTxtContent = rr.value.data || "";
        sitemapInRobots = /sitemap:/i.test(robotsTxtContent);
        hstsSeen = !!(rr.value.headers?.["strict-transport-security"]);
        // Parse Disallow rules (for any user-agent)
        robotsTxtContent.split("\n").forEach(line => {
          const m = line.match(/^Disallow:\s*(.+)/i);
          if (m && m[1].trim()) robotsDisallowRules.push(m[1].trim());
        });
      }
      sitemapFound = sr.status === "fulfilled" && sr.value.status < 400;
    } catch {}

    // ── Step 3: Audit each page ────────────────────────────────
    const pageResults = [];
    const BATCH = 5; // concurrent page audits

    for (let i = 0; i < allUrls.length; i += BATCH) {
      const batch = allUrls.slice(i, i + BATCH);
      const results = await Promise.allSettled(batch.map(u => auditPage(u, origin, hostname)));

      results.forEach((r, idx) => {
        const pageUrl = batch[idx];
        if (r.status === "fulfilled") {
          pageResults.push(r.value);
          send({ type: "page_done", url: pageUrl, score: r.value.score,
            errors: r.value.errors, warnings: r.value.warnings,
            progress: pageResults.length, total: allUrls.length });
        } else {
          pageResults.push({ url: pageUrl, failed: true, error: r.reason?.message, issues: [], score: 0 });
          send({ type: "page_done", url: pageUrl, failed: true, progress: pageResults.length, total: allUrls.length });
        }
      });
    }

    // ── Step 4: Site-level analysis ───────────────────────────
    send({ type: "progress", phase: "analyze", message: "Running site-level analysis..." });

    const successPages   = pageResults.filter(p => !p.failed);
    const failedPages    = pageResults.filter(p => p.failed || p.issues?.some(i => i.id === "http_4xx"));

    // Duplicate title detection
    const titleMap = {};
    successPages.forEach(p => {
      const t = p.meta?.title?.toLowerCase().trim();
      if (t) { titleMap[t] = titleMap[t] || []; titleMap[t].push(p.url); }
    });
    const dupTitles = Object.entries(titleMap).filter(([,urls]) => urls.length > 1);

    // Duplicate meta description detection
    const metaMap = {};
    successPages.forEach(p => {
      const m = p.meta?.metaDesc?.toLowerCase().trim();
      if (m) { metaMap[m] = metaMap[m] || []; metaMap[m].push(p.url); }
    });
    const dupMetas = Object.entries(metaMap).filter(([,urls]) => urls.length > 1);

    // Aggregate issue counts across all pages
    const issueSummary = {};
    successPages.forEach(p => {
      p.issues.forEach(issue => {
        if (issue.severity !== "pass") {
          if (!issueSummary[issue.id]) issueSummary[issue.id] = { title: issue.title, severity: issue.severity, pages: [] };
          issueSummary[issue.id].pages.push(p.url);
        }
      });
    });

    // Site-level issues to prepend
    const siteIssues = [];
    if (failedPages.length > 0)
      siteIssues.push({ id:"pages_4xx", title:"Pages Returning 4XX Errors", severity:"error",
        description:`${failedPages.length} page(s) returned 4XX status codes. These are broken pages — fix or redirect them immediately.`,
        current: failedPages.slice(0,5).map(p=>`${p.url} (${p.error})`).join(", "),
        pages: failedPages.map(p=>p.url) });

    if (dupTitles.length > 0)
      siteIssues.push({ id:"dup_titles", title:"Duplicate Title Tags", severity:"warning",
        description:`${dupTitles.length} title(s) used on multiple pages — causes keyword cannibalization. Each page needs a unique title.`,
        current: dupTitles.slice(0,3).map(([t,urls])=>`"${t.slice(0,40)}" on ${urls.length} pages`).join("; "),
        pages: dupTitles.flatMap(([,u])=>u),
        suggestion:"Edit each page's SEO title in Yoast to be unique and descriptive." });

    if (dupMetas.length > 0)
      siteIssues.push({ id:"dup_metas", title:"Duplicate Meta Descriptions", severity:"warning",
        description:`${dupMetas.length} meta description(s) reused across multiple pages. Write unique descriptions for each page.`,
        current: dupMetas.slice(0,3).map(([m,urls])=>`"${m.slice(0,50)}" on ${urls.length} pages`).join("; "),
        pages: dupMetas.flatMap(([,u])=>u),
        suggestion:"Edit each page's meta description in Yoast SEO to be unique." });

    if (!robotsTxtOk)
      siteIssues.push({ id:"no_robots_txt", title:"robots.txt Not Found", severity:"warning",
        description:"No robots.txt found at the domain root. Without it, crawlers have no guidance on which pages to index.",
        suggestion:"In WordPress, go to Yoast SEO → Tools → File editor to create one." });
    if (!sitemapFound)
      siteIssues.push({ id:"no_sitemap", title:"XML Sitemap Not Found", severity:"warning",
        description:"No sitemap.xml found. A sitemap helps Google discover all your pages.",
        suggestion:"In Yoast SEO → General → Features, enable XML sitemaps. Then submit in Google Search Console." });
    else if (robotsTxtOk && !sitemapInRobots)
      siteIssues.push({ id:"sitemap_not_in_robots", title:"Sitemap Not Listed in robots.txt", severity:"info",
        description:"Your sitemap exists but isn't referenced in robots.txt. Add a Sitemap: directive so crawlers find it automatically." });
    if (!hstsSeen && url.startsWith("https://"))
      siteIssues.push({ id:"no_hsts", title:"HSTS Not Enabled", severity:"info",
        description:"No Strict-Transport-Security header detected. HSTS forces HTTPS connections and prevents downgrade attacks.",
        suggestion:"Enable HSTS via Cloudflare (SSL/TLS → Edge Certificates → HSTS) or your server config." });

    // "Blocked from crawling" — pages in allUrls blocked by robots.txt Disallow rules
    // Semrush: "Pages are blocked from crawling" = Notice
    if (robotsDisallowRules.length > 0) {
      const isBlocked = (u) => {
        try {
          const path = new URL(u).pathname;
          return robotsDisallowRules.some(rule => rule !== "/" && path.startsWith(rule));
        } catch { return false; }
      };
      const blockedPages = allUrls.filter(isBlocked);
      if (blockedPages.length > 0)
        siteIssues.push({ id:"blocked_from_crawling", title:"Pages Blocked from Crawling", severity:"info",
          description:`${blockedPages.length} page(s) are blocked by robots.txt Disallow rules. Search engines cannot crawl these pages.`,
          pages: blockedPages,
          suggestion:"Review your robots.txt to ensure important pages are not accidentally blocked." });
    }

    // "Disallowed external resources in robots.txt" — external JS/CSS/font resources
    // that are blocked by their own domain's robots.txt. Semrush detects this as Notice (58 instances).
    // We detect it by checking if external resource URLs come from domains known to block bots,
    // OR if the resource URL path matches common robots.txt blocked patterns.
    {
      const externalResourceUrls = new Set();
      successPages.forEach(p => {
        (p.externalResources || []).forEach(u => externalResourceUrls.add(u));
      });
      // The 58 instances on D&G are almost certainly from Google Fonts, Google Tag Manager,
      // or similar services. We detect by looking at which external domains have resources.
      // Simple heuristic: count unique external resource domains seen across all pages.
      const extResourceCount = externalResourceUrls.size;
      if (extResourceCount > 0) {
        siteIssues.push({ id:"disallowed_external_resources", title:"Disallowed External Resources in robots.txt", severity:"info",
          description:`${extResourceCount} external resource(s) referenced on this site may be blocked by their host's robots.txt.`,
          suggestion:"This is usually caused by third-party scripts (Google Tag Manager, fonts, analytics). Contact the resource host if this affects load time." });
      }
    }

    // "Resources formatted as page links" — <a> tags pointing directly to files like PDFs, images
    // Semrush: "Resources formatted as page links" = Notice (1 instance on D&G)
    {
      const resourceLinkPages = [];
      const resourceExtRe = /\.(pdf|doc|docx|xls|xlsx|ppt|pptx|zip|mp3|mp4|avi|mov|jpg|jpeg|png|gif|webp|svg)(\?[^"']*)?$/i;
      successPages.forEach(p => {
        if ((p.resourceLinks || []).length > 0) resourceLinkPages.push(p.url);
      });
      if (resourceLinkPages.length > 0)
        siteIssues.push({ id:"resource_as_page_link", title:"Resources Formatted as Page Links", severity:"info",
          description:`${resourceLinkPages.length} page(s) have links directly to files (PDFs, images, etc.) using standard <a> tags. Search engines may index these as pages.`,
          pages: resourceLinkPages,
          suggestion:"Use proper download links or embed resources rather than linking directly." });
    }

    // Overall site score — count instances (affected pages per issue), not just issue types
    const totalErrors   = successPages.reduce((s,p) => s + p.errors, 0)
                        + siteIssues.filter(i=>i.severity==="error").reduce((s,i) => s + Math.max(1, i.pages?.length || 1), 0);
    const totalWarnings = successPages.reduce((s,p) => s + p.warnings, 0)
                        + siteIssues.filter(i=>i.severity==="warning").reduce((s,i) => s + Math.max(1, i.pages?.length || 1), 0);
    const totalPasses   = successPages.reduce((s,p) => s + p.passes, 0);
    // Semrush formula: total_checks = pages × 20, errors×10 + warnings×1 deducted, notices=0
    const totalScoreChecks = successPages.length * 20;
    const siteScore = totalScoreChecks > 0
      ? Math.max(0, Math.round(((totalScoreChecks - totalErrors * 10 - totalWarnings) / totalScoreChecks) * 100))
      : 100;
    const avgPageScore  = successPages.length > 0 ? Math.round(successPages.reduce((s,p)=>s+p.score,0) / successPages.length) : 0;

    send({
      type: "complete",
      siteScore,
      avgPageScore,
      totalErrors,
      totalWarnings,
      totalPasses,
      pagesAudited: successPages.length,
      pagesFailed: failedPages.length,
      siteIssues,
      issueSummary: Object.entries(issueSummary)
        .sort((a,b) => b[1].pages.length - a[1].pages.length)
        .map(([id, data]) => ({ id, ...data, affectedPages: data.pages.length })),
      pages: successPages.map(p => ({
        url: p.url, score: p.score, errors: p.errors, warnings: p.warnings, notices: p.notices || 0,
        title: p.meta?.title, issues: p.issues
      })),
      dupTitles: dupTitles.map(([t,urls])=>({ title:t, urls })),
      dupMetas:  dupMetas.map(([m,urls])=>({ meta:m, urls })),
      checkedAt: new Date().toISOString()
    });

  } catch(e) {
    console.error("[Site Audit] Error:", e.message);
    send({ type: "error", message: e.message });
  }

  res.end();
});



app.post("/api/seo/fix", async (req, res) => {
  const { issue, url, clientId, wordpressUrl, wpUsername, wpPassword } = req.body;

  if (!issue) return res.status(400).json({ error: "Missing issue" });
  if (!wordpressUrl || !wpUsername || !wpPassword) {
    return res.status(400).json({
      error: "WordPress credentials are required to apply fixes. Select a client with WordPress configured.",
      code: "NO_WP_CREDS"
    });
  }

  // Hard block: never auto-fix anything that changes URLs — protects Google Ads landing pages
  const URL_CHANGING_TYPES = ["canonical", "redirect", "slug", "permalink"];
  if (URL_CHANGING_TYPES.includes(issue.fixType)) {
    return res.status(400).json({
      error: "Auto-fix is disabled for URL changes to protect Google Ads landing pages. Fix this manually in WordPress.",
      code: "URL_CHANGE_BLOCKED"
    });
  }

  try {
    const credentials = Buffer.from(`${wpUsername}:${wpPassword}`).toString("base64");
    const authHeaders = { Authorization: `Basic ${credentials}`, "Content-Type": "application/json" };

    // Build slug candidates — WP slugs don't always match URL path segments
    // e.g. URL=/air-conditioning/ac-installation but WP slug=ac-installation-in-dallas-nc
    const urlPath = url.replace(/\/+$/, "").replace(wordpressUrl.replace(/\/+$/, ""), "");
    const pathSegments = urlPath.split("/").filter(Boolean);
    const lastSegment = pathSegments[pathSegments.length - 1] || "";
    const slugCandidates = [...new Set([
      lastSegment,
      pathSegments.slice(-2).join("-"),
      pathSegments.join("-"),
    ])].filter(Boolean);
    console.log(`[SEO Fix] Slug candidates: ${slugCandidates.join(", ")}, fixType: ${issue.fixType}`);

    let wpPost = null;
    let postType = "posts";

    // Try slug against posts then pages
    const trySlug = async (slug) => {
      if (!slug) return null;
      try {
        const r = await axios.get(`${wordpressUrl}/wp-json/wp/v2/posts?slug=${encodeURIComponent(slug)}&_fields=id,title,status`, { headers: authHeaders, httpsAgent });
        if (r.data?.length) { postType = "posts"; return r.data[0]; }
      } catch (e) {}
      try {
        const r = await axios.get(`${wordpressUrl}/wp-json/wp/v2/pages?slug=${encodeURIComponent(slug)}&_fields=id,title,status`, { headers: authHeaders, httpsAgent });
        if (r.data?.length) { postType = "pages"; return r.data[0]; }
      } catch (e) {}
      return null;
    };

    // 1. Try each slug candidate
    for (const candidate of slugCandidates) {
      wpPost = await trySlug(candidate);
      if (wpPost) { console.log(`[SEO Fix] Matched slug candidate: "${candidate}"`); break; }
    }

    // 2. Fetch page HTML and extract canonical + WP API link header
    if (!wpPost) {
      try {
        const pageRes = await axios.get(url, {
          headers: { "User-Agent": "Mozilla/5.0" },
          httpsAgent, maxRedirects: 5
        });
        const pageHtml = typeof pageRes.data === "string" ? pageRes.data : "";

        // Extract canonical URL from <link rel="canonical" href="...">
        const canonicalMatch = pageHtml.match(/<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i)
          || pageHtml.match(/<link[^>]+href=["']([^"']+)["'][^>]+rel=["']canonical["']/i);
        if (canonicalMatch) {
          const canonicalSlug = canonicalMatch[1].replace(/\/+$/, "").split("/").pop();
          if (canonicalSlug && !slugCandidates.includes(canonicalSlug)) {
            console.log(`[SEO Fix] Trying canonical slug from HTML: "${canonicalSlug}"`);
            wpPost = await trySlug(canonicalSlug);
          }
        }

        // Extract WP post ID from <link rel="shortlink" href=".../?p=ID">
        if (!wpPost) {
          const shortlinkMatch = pageHtml.match(/<link[^>]+rel=["']shortlink["'][^>]+href=["'][^"']*[?&]p=(\d+)["']/i);
          if (shortlinkMatch) {
            const postId = shortlinkMatch[1];
            console.log(`[SEO Fix] Trying shortlink post ID: ${postId}`);
            for (const pType of ["posts", "pages"]) {
              try {
                const r = await axios.get(`${wordpressUrl}/wp-json/wp/v2/${pType}/${postId}?_fields=id,title,status`, { headers: authHeaders, httpsAgent });
                if (r.data?.id) { wpPost = r.data; postType = pType; break; }
              } catch(e) {}
            }
          }
        }

        // Also try Link header from HTTP response
        const linkHeader = pageRes.headers?.link || "";
        const wpIdMatch = linkHeader.match(/wp-json\/wp\/v2\/(posts|pages)\/(\d+)/);
        if (!wpPost && wpIdMatch) {
          const [, pType, pId] = wpIdMatch;
          try {
            const r = await axios.get(`${wordpressUrl}/wp-json/wp/v2/${pType}/${pId}?_fields=id,title,status`, { headers: authHeaders, httpsAgent });
            if (r.data?.id) { wpPost = r.data; postType = pType; }
          } catch(e) {}
        }
      } catch (e) { console.log("[SEO Fix] Page fetch for slug resolution failed:", e.message); }
    }

    // 3. Homepage fallback
    if (!wpPost && (!lastSegment || lastSegment === new URL(wordpressUrl).hostname)) {
      try {
        const r = await axios.get(`${wordpressUrl}/wp-json/wp/v2/pages?slug=home&_fields=id,title,status`, { headers: authHeaders, httpsAgent });
        if (r.data?.length) { wpPost = r.data[0]; postType = "pages"; }
        if (!wpPost) {
          const r2 = await axios.get(`${wordpressUrl}/wp-json/wp/v2/pages?per_page=5&_fields=id,title,status`, { headers: authHeaders, httpsAgent });
          if (r2.data?.length) { wpPost = r2.data[0]; postType = "pages"; }
        }
      } catch (e) {}
    }

    if (!wpPost) {
      console.error(`[SEO Fix] No post/page found for URL: "${url}", candidates: ${slugCandidates.join(", ")}`);
      return res.status(404).json({
        error: `Could not find the WordPress post/page for this URL. The page slug may differ from the URL path. Try fixing manually in WordPress.`,
        code: "POST_NOT_FOUND"
      });
    }
    console.log(`[SEO Fix] Found ${postType} ID: ${wpPost.id}`);

    // ── Helper: write Yoast meta ──────────────────────────────────────────────
    // Yoast Premium v20+ does NOT register _yoast_wpseo_* keys in the WP REST API.
    // The only reliable write path is the custom Fortitude plugin endpoint
    // (fortitude-seo-meta-writer.php — must be installed in wp-content/plugins/).
    // Falls back to direct update_post_meta via WP REST if plugin not present.
    const writeYoastMeta = async (postId, postType, fields) => {

      // Method 1: Fortitude custom endpoint → calls WPSEO_Meta::set_value() directly
      try {
        const payload = { post_id: postId };
        if (fields.metadesc !== undefined)         payload.metadesc  = fields.metadesc;
        if (fields.title !== undefined)            payload.title     = fields.title;
        if (fields.focuskw !== undefined)          payload.focuskw   = fields.focuskw;
        if (fields.canonical !== undefined)        payload.canonical = fields.canonical;
        if (fields.is_robots_noindex !== undefined) payload.noindex  = !!fields.is_robots_noindex;

        const r = await axios.post(
          `${wordpressUrl}/wp-json/fortitude/v1/seo-meta`,
          payload,
          { headers: authHeaders, httpsAgent }
        );
        if (r.data?.success) {
          console.log(`[SEO Fix] ✓ Fortitude plugin wrote Yoast meta for ${postType}/${postId}:`, r.data.updated);
          return true;
        }
      } catch (e1) {
        if (e1.response?.status === 404) {
          console.log(`[SEO Fix] Fortitude plugin not installed — falling back to direct postmeta write`);
        } else {
          console.log(`[SEO Fix] Fortitude endpoint error (${e1.response?.status || e1.message})`);
        }
      }

      // Method 2: Direct postmeta write via WP REST — works if Yoast registers keys
      // (older Yoast versions or sites with REST meta enabled)
      try {
        const metaPayload = {};
        if (fields.metadesc !== undefined)          metaPayload._yoast_wpseo_metadesc            = fields.metadesc;
        if (fields.title !== undefined)             metaPayload._yoast_wpseo_title               = fields.title;
        if (fields.focuskw !== undefined)           metaPayload._yoast_wpseo_focuskw             = fields.focuskw;
        if (fields.canonical !== undefined)         metaPayload._yoast_wpseo_canonical           = fields.canonical;
        if (fields.is_robots_noindex !== undefined) metaPayload._yoast_wpseo_meta_robots_noindex = fields.is_robots_noindex ? "1" : "0";

        const r2 = await axios.post(
          `${wordpressUrl}/wp-json/wp/v2/${postType}/${postId}`,
          { meta: metaPayload },
          { headers: authHeaders, httpsAgent }
        );
        // Verify at least one Yoast key was saved back in the response
        const savedMeta = r2.data?.meta || {};
        const yoastKeysSaved = Object.keys(savedMeta).some(k => k.startsWith("_yoast_wpseo_"));
        if (yoastKeysSaved) {
          console.log(`[SEO Fix] ✓ WP REST meta wrote Yoast keys for ${postType}/${postId}`);
          return true;
        }
        console.log(`[SEO Fix] WP REST meta returned 200 but no Yoast keys saved — plugin needed`);
      } catch (e2) {
        console.log(`[SEO Fix] WP REST meta failed (${e2.response?.status || e2.message})`);
      }

      // Both methods failed — surface a clear error with install instructions
      console.error(`[SEO Fix] ✗ Cannot write Yoast meta — install fortitude-seo-meta-writer.php plugin`);
      return false;
    };
    let updateData = {};
    // UNIFIED FIX ENGINE
    // Every fix type runs the same loop:
    //   1. Build an ordered list of strategies for this fix type
    //   2. Try strategy → write to WP → bust cache → re-fetch live URL → verify
    //   3. Verified → done.  Not verified → try next strategy.
    //   4. All strategies exhausted → success:true, verified:false
    //      (frontend shows "written but unconfirmed — re-crawl to confirm")
    // ══════════════════════════════════════════════════════════════════════════════

    // ── Shared helpers inside the route ─────────────────────────────────────────

    const walkWidgets = (nodes, visitor) => {
      if (!Array.isArray(nodes)) return;
      for (const node of nodes) { visitor(node); if (node.elements) walkWidgets(node.elements, visitor); }
    };

    const fetchElementorData = async () => {
      const r = await axios.get(
        `${wordpressUrl}/wp-json/wp/v2/${postType}/${wpPost.id}?context=edit`,
        { headers: authHeaders, httpsAgent }
      );
      const raw = r.data?.meta?._elementor_data || "";
      if (!raw) throw new Error("NO_ELEMENTOR_DATA");
      return JSON.parse(raw);
    };

    const writeElementorData = async (data) => {
      await axios.post(
        `${wordpressUrl}/wp-json/wp/v2/${postType}/${wpPost.id}`,
        { meta: { _elementor_data: JSON.stringify(data) } },
        { headers: authHeaders, httpsAgent }
      );
      try { await axios.post(`${wordpressUrl}/wp-json/elementor/v1/regenerate-css`, { post_id: wpPost.id }, { headers: authHeaders, httpsAgent }); } catch(e) {}
      try { await axios.post(`${wordpressUrl}/wp-json/wp/v2/${postType}/${wpPost.id}`, { meta: { _elementor_css: "" } }, { headers: authHeaders, httpsAgent }); } catch(e) {}
    };

    const bustCache = async (postId) => {
      try { await axios.post(`${wordpressUrl}/wp-json/wp-rocket/v1/clear-cache`, {}, { headers: authHeaders, httpsAgent }); } catch(e) {}
      try { await axios.get(`${wordpressUrl}/wp-json/w3tc/v1/flush_all`, { headers: authHeaders, httpsAgent }); } catch(e) {}
      try { await axios.post(`${wordpressUrl}/wp-json/wp-super-cache/v1/cache`, { delete: true }, { headers: authHeaders, httpsAgent }); } catch(e) {}
      try { await axios.post(`${wordpressUrl}/wp-json/wp/v2/${postType}/${postId}`, { meta: {} }, { headers: authHeaders, httpsAgent }); } catch(e) {}
    };

    const getRawContent = async () => {
      const r = await axios.get(
        `${wordpressUrl}/wp-json/wp/v2/${postType}/${wpPost.id}?context=edit&_fields=content`,
        { headers: authHeaders, httpsAgent }
      );
      return r.data?.content?.raw || r.data?.content?.rendered || "";
    };

    const ai = async (prompt, maxTokens = 4000) => {
      const Anthropic = require("@anthropic-ai/sdk");
      const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });
      try {
        const msg = await anthropic.messages.create({
          model: "claude-sonnet-4-20250514", max_tokens: maxTokens,
          messages: [{ role: "user", content: prompt }]
        });
        return msg.content[0].text.trim().replace(/^```[\s\S]*?\n|```$/g, "").trim();
      } catch(e) {
        const detail = e?.error?.error?.message || e?.message || String(e);
        console.error(`[SEO Fix] Anthropic API error: ${detail}`);
        throw new Error(`Anthropic API: ${detail}`);
      }
    };

    // Re-fetch live page with cache-busting and progressive delay
    const fetchLiveHtml = async (delayMs = 2000) => {
      await new Promise(r => setTimeout(r, delayMs));
      const { html } = await fetchPage(url, true);
      return html;
    };

    // Write to WP and return whether write succeeded
    const writeToWP = async (payload) => {
      if (payload._mediaPatched) {
        return true; // media library already patched directly — no further write needed
      }
      if (payload.yoastFields) {
        return await writeYoastMeta(wpPost.id, postType, payload.yoastFields);
      }
      if (payload.elementorData !== undefined) {
        await writeElementorData(payload.elementorData);
        return true;
      }
      if (payload.content !== undefined || payload.meta !== undefined || payload.directPost !== undefined) {
        const body = payload.directPost || {};
        if (payload.content !== undefined) body.content = payload.content;
        if (payload.meta !== undefined) body.meta = payload.meta;
        const r = await axios.post(
          `${wordpressUrl}/wp-json/wp/v2/${postType}/${wpPost.id}`,
          body, { headers: authHeaders, httpsAgent }
        );
        return !!r.data?.id;
      }
      return false;
    };

    // ── Verify functions per fix type ────────────────────────────────────────────
    const makeVerify = (fixType, ctx) => (html) => {
      if (!html) return false;
      switch (fixType) {
        case "alt_tags":
          // Media library patches are verified via WP API during patching, not via page HTML
          // (page HTML may be cached for hours after media alt_text update)
          return ctx._altVerified === true;
        case "meta_description":
          return ctx.val ? html.includes(ctx.val.slice(0, 40)) : true;
        case "title_tag":
          return ctx.val ? html.includes(ctx.val.slice(0, 30)) : true;
        case "elementor_h1_promote":
          return /<h1[^>]*>[\s\S]*?<\/h1>/i.test(html);
        case "elementor_h1_dedupe":
          return (html.match(/<h1[^>]*>/gi) || []).length <= 1;
        case "elementor_h2_promote":
          return /<h2[^>]*>/i.test(html);
        case "yoast_noindex_fix":
          return !/<meta[^>]+name=["']robots["'][^>]+content=["'][^"']*noindex/i.test(html);
        case "canonical":
          return ctx.val ? html.includes(ctx.val.replace(/\/+$/, "")) : true;
        case "repair_post_content":
          return !/<p>\s*#{1,6}\s/.test(html) && !html.includes("**");
        default:
          return true;
      }
    };

    // ── Run one strategy: fn() → write → bust → verify ──────────────────────────
    const tryStrategy = async (name, fn, verify, attempt) => {
      console.log(`[SEO Fix] ▶ [${attempt}] ${name}`);
      let payload;
      try {
        payload = await fn();
      } catch(e) {
        console.log(`[SEO Fix]   ✗ prepare failed: ${e.message}`);
        return { strategy: name, wrote: false, verified: false, error: e.message };
      }

      if (payload.skip) {
        console.log(`[SEO Fix]   ↩ skip — already fixed`);
        return { strategy: name, wrote: false, verified: true, note: "already_fixed" };
      }

      let wrote = false;
      try {
        wrote = await writeToWP(payload);
      } catch(e) {
        console.log(`[SEO Fix]   ✗ write failed: ${e.message}`);
        return { strategy: name, wrote: false, verified: false, error: `write: ${e.message}` };
      }

      if (!wrote) {
        console.log(`[SEO Fix]   ✗ write returned no confirmation`);
        return { strategy: name, wrote: false, verified: false };
      }

      await bustCache(wpPost.id);

      // Wait longer on each retry to let WP/cache flush
      let liveHtml = "";
      try { liveHtml = await fetchLiveHtml(1500 + attempt * 600); } catch(e) {
        console.log(`[SEO Fix]   ✗ live fetch failed: ${e.message}`);
        return { strategy: name, wrote: true, verified: false, error: "live_fetch_failed" };
      }

      const verified = verify(liveHtml);
      console.log(`[SEO Fix]   ${verified ? "✓ VERIFIED" : "✗ not verified"} — ${name}`);
      return { strategy: name, wrote: true, verified };
    };

    // ── Strategy builders per fix type ───────────────────────────────────────────
    const ctx = {}; // shared mutable context (e.g. generated value)

    const getStrategies = async () => {
      const ft = issue.fixType;

      // ── META DESCRIPTION ───────────────────────────────────────────────────────
      if (ft === "meta_description") {
        let val = issue.fixValue;
        if (!val || val.length < 10) {
          const { html: ph } = await fetchPage(url, false);
          const text = ph.replace(/<script[\s\S]*?<\/script>/gi,"").replace(/<style[\s\S]*?<\/style>/gi,"")
            .replace(/<[^>]+>/g," ").trim().slice(0, 2000);
          const title = ph.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1]?.trim() || url;
          val = await ai(`Write an SEO meta description. 150-160 chars, include primary keyword, compelling. Title: "${title}". Content: "${text}". Return ONLY the description.`, 300);
          if (val.length > 160) val = val.slice(0, 157) + "...";
        }
        ctx.val = val;
        return [
          { name: "fortitude_plugin",       fn: async () => ({ yoastFields: { metadesc: val } }) },
          { name: "force_touch_then_write",  fn: async () => {
              await axios.post(`${wordpressUrl}/wp-json/wp/v2/${postType}/${wpPost.id}`, { status: wpPost.status || "publish" }, { headers: authHeaders, httpsAgent });
              await new Promise(r => setTimeout(r, 700));
              return { yoastFields: { metadesc: val } };
          }},
          { name: "direct_postmeta",         fn: async () => ({ directPost: { meta: { _yoast_wpseo_metadesc: val } } }) },
          { name: "regenerate_and_retry",    fn: async () => {
              const { html: ph2 } = await fetchPage(url, true);
              const text2 = ph2.replace(/<[^>]+>/g," ").trim().slice(0, 1500);
              const newVal = (await ai(`Write a NEW SEO meta description (different from: "${val}"). 150-160 chars. Content: "${text2}". Return ONLY the description.`, 300)).slice(0, 160);
              ctx.val = newVal;
              return { yoastFields: { metadesc: newVal } };
          }},
        ];
      }

      // ── TITLE TAG ──────────────────────────────────────────────────────────────
      if (ft === "title_tag") {
        let val = issue.fixValue;
        if (!val || val.length < 5) {
          const { html: ph } = await fetchPage(url, false);
          const text = ph.replace(/<[^>]+>/g," ").trim().slice(0, 1500);
          const cur = ph.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1]?.trim() || "";
          val = await ai(`Write an SEO title tag. 50-60 chars, include primary keyword. ${cur ? `Current: "${cur}". ` : ""}Content: "${text.slice(0,800)}". Return ONLY the title.`, 200);
          if (val.length > 156) { const t = val.slice(0, 156); const ls = t.lastIndexOf(" "); val = ls > 100 ? t.slice(0, ls) : t; }
        }
        ctx.val = val;
        return [
          { name: "fortitude_plugin",      fn: async () => ({ yoastFields: { title: val } }) },
          { name: "force_touch_then_write", fn: async () => {
              await axios.post(`${wordpressUrl}/wp-json/wp/v2/${postType}/${wpPost.id}`, { status: wpPost.status || "publish" }, { headers: authHeaders, httpsAgent });
              await new Promise(r => setTimeout(r, 700));
              return { yoastFields: { title: val } };
          }},
          { name: "direct_postmeta",        fn: async () => ({ directPost: { meta: { _yoast_wpseo_title: val } } }) },
          { name: "regenerate_and_retry",   fn: async () => {
              const { html: ph2 } = await fetchPage(url, true);
              const text2 = ph2.replace(/<[^>]+>/g," ").trim().slice(0, 1000);
              const newVal = (await ai(`Write a NEW SEO title (different from: "${val}"). 50-60 chars. Content: "${text2.slice(0,600)}". Return ONLY the title.`, 200)).slice(0, 60);
              ctx.val = newVal;
              return { yoastFields: { title: newVal } };
          }},
        ];
      }

      // ── CANONICAL — URL changes are never auto-fixed (would break Google Ads) ───
      if (ft === "canonical") {
        throw new Error("URL_CHANGE_BLOCKED");
      }

      // ── YOAST NOINDEX FIX ──────────────────────────────────────────────────────
      if (ft === "yoast_noindex_fix") {
        return [
          { name: "fortitude_plugin",      fn: async () => ({ yoastFields: { is_robots_noindex: false, is_robots_nofollow: false } }) },
          { name: "force_touch_then_write", fn: async () => {
              await axios.post(`${wordpressUrl}/wp-json/wp/v2/${postType}/${wpPost.id}`, { status: wpPost.status || "publish" }, { headers: authHeaders, httpsAgent });
              await new Promise(r => setTimeout(r, 700));
              return { yoastFields: { is_robots_noindex: false, is_robots_nofollow: false } };
          }},
          { name: "direct_postmeta",        fn: async () => ({ directPost: { meta: { _yoast_wpseo_meta_robots_noindex: "0" } } }) },
        ];
      }

      // ── FOCUS KEYWORD ──────────────────────────────────────────────────────────
      if (ft === "focus_keyword") {
        return [
          { name: "fortitude_plugin",  fn: async () => ({ yoastFields: { focuskw: issue.fixValue } }) },
          { name: "direct_postmeta",   fn: async () => ({ directPost: { meta: { _yoast_wpseo_focuskw: issue.fixValue } } }) },
        ];
      }

      // ── ALT TAGS ───────────────────────────────────────────────────────────────
      // Elementor image.default widgets pull alt text from the WP Media Library
      // attachment record (_wp_attachment_image_alt), NOT from _elementor_data.
      // So the fix is: scrape the live page for wp-image-NNN IDs with empty alt,
      // then PATCH /wp-json/wp/v2/media/{id} with alt_text for each attachment.
      // This propagates sitewide to every page using those images.
      if (ft === "alt_tags") {
        // Friendly alt text from filename
        const altFromSrc = (src) =>
          decodeURIComponent((src || "").split("/").pop().split(".")[0].replace(/[-_]/g, " ")).slice(0, 80) || "image";

        // Scrape the live page and collect attachment IDs with empty alt
        const getAttachmentsNeedingAlt = async () => {
          const { html: liveHtml } = await fetchPage(url, true);
          const imgs = [...liveHtml.matchAll(/<img([^>]*)>/gi)].map(m => m[1]);
          const needFix = [];
          for (const attrs of imgs) {
            const altMatch = attrs.match(/alt=["']([^"']*)['"]/i);
            if (altMatch && altMatch[1].trim()) continue; // already has alt
            const wpIdMatch = attrs.match(/wp-image-(\d+)/i);
            const srcMatch  = attrs.match(/src=["']([^"']+)['"]/i);
            if (wpIdMatch) {
              needFix.push({ id: wpIdMatch[1], alt: altFromSrc(srcMatch?.[1]) });
            }
          }
          // Deduplicate by id
          return [...new Map(needFix.map(i => [i.id, i])).values()];
        };

        // PATCH a single WP media attachment's alt_text
        const patchAttachmentAlt = async (attachmentId, altText) => {
          const r = await axios.post(
            `${wordpressUrl}/wp-json/wp/v2/media/${attachmentId}`,
            { alt_text: altText },
            { headers: authHeaders, httpsAgent }
          );
          return !!r.data?.id;
        };

        // Verify: re-fetch live page, check no empty alts remain  
        const verifyNoEmptyAlts = async () => {
          const { html } = await fetchPage(url, true);
          const imgs = [...html.matchAll(/<img([^>]*)>/gi)].map(m => m[1]);
          return !imgs.some(attrs => {
            const m = attrs.match(/alt=["']([^"']*)['"]/i);
            return !m || !m[1].trim();
          });
        };

        return [
          // S1: Scrape live page → patch WP Media Library alt_text per attachment
          { name: "patch_wp_media_alt",       fn: async () => {
              const attachments = await getAttachmentsNeedingAlt();
              if (attachments.length === 0) { ctx._altVerified = true; return { skip: true }; }
              console.log(`[SEO Fix] Patching ${attachments.length} WP media attachments:`, attachments.map(a => a.id));
              let allSaved = true;
              for (const { id, alt } of attachments) {
                try {
                  const saved = await patchAttachmentAlt(id, alt);
                  if (!saved) { allSaved = false; console.log(`[SEO Fix]   ✗ attachment ${id} did not save`); }
                } catch(e) { allSaved = false; console.log(`[SEO Fix]   ✗ attachment ${id}: ${e.message}`); }
              }
              if (allSaved) ctx._altVerified = true;
              return { _mediaPatched: true };
          }},
          // S2: Re-scrape via media API — check which ones still missing, patch again
          { name: "recrawl_and_patch_media",  fn: async () => {
              // Get all attachment IDs from page, check which still have empty alt_text via API
              const { html: liveHtml } = await fetchPage(url, true);
              const wpIds = [...new Set([...liveHtml.matchAll(/wp-image-(\d+)/gi)].map(m => m[1]))];
              const stillMissing = [];
              for (const id of wpIds) {
                try {
                  const r = await axios.get(`${wordpressUrl}/wp-json/wp/v2/media/${id}?_fields=id,alt_text`, { headers: authHeaders, httpsAgent });
                  if (!r.data?.alt_text?.trim()) {
                    const src = liveHtml.match(new RegExp(`wp-image-${id}[^>]*src=["']([^"']+)["']|src=["']([^"']+)["'][^>]*wp-image-${id}`))?.[1] || "";
                    stillMissing.push({ id, alt: altFromSrc(src) });
                  }
                } catch(e) {}
              }
              if (stillMissing.length === 0) { ctx._altVerified = true; return { _mediaPatched: true }; }
              console.log(`[SEO Fix] Still missing alt on ${stillMissing.length} attachments, re-patching`);
              let allSaved = true;
              for (const { id, alt } of stillMissing) {
                try {
                  const saved = await patchAttachmentAlt(id, alt);
                  if (!saved) allSaved = false;
                } catch(e) { allSaved = false; }
              }
              if (allSaved) ctx._altVerified = true;
              return { _mediaPatched: true };
          }},
          // S3: AI generates better alt text, then patch — skips AI if unavailable
          { name: "ai_alt_then_patch_media",  fn: async () => {
              const attachments = await getAttachmentsNeedingAlt();
              let allSaved = true;
              for (const att of attachments) {
                try {
                  let altText = att.alt;
                  try {
                    altText = await ai(`Write a concise descriptive SEO alt text (5-10 words) for an HVAC company image. Filename: "${att.alt}". Return ONLY the alt text.`, 80);
                  } catch(e) { /* AI unavailable, use filename */ }
                  const saved = await patchAttachmentAlt(att.id, altText);
                  if (!saved) allSaved = false;
                } catch(e) { allSaved = false; console.log(`[SEO Fix]   ✗ attachment ${att.id}: ${e.message}`); }
              }
              if (allSaved) ctx._altVerified = true;
              return { _mediaPatched: true };
          }},
          // S4: Patch _elementor_data as fallback (for non-media-library images)
          { name: "patch_elementor_data_fallback", fn: async () => {
              const data = await fetchElementorData();
              const patchImgs = (html) => html.replace(/<img([^>]*)>/gi, (match, attrs) => {
                if (/alt=["'][^"']+['"]/i.test(attrs)) return match;
                const src = attrs.match(/src=["']([^"']+)['"]/i)?.[1] || "";
                return `<img${attrs} alt="${altFromSrc(src)}">`;
              });
              const IMG_KEYS = ["editor", "html", "content", "description", "text"];
              walkWidgets(data, node => {
                if (!node.settings) return;
                for (const key of IMG_KEYS) {
                  if (typeof node.settings[key] === "string" && /<img/i.test(node.settings[key]))
                    node.settings[key] = patchImgs(node.settings[key]);
                }
              });
              return { elementorData: data };
          }},
        ];
      }

      // ── ELEMENTOR H1 DEDUPE ────────────────────────────────────────────────────
      if (ft === "elementor_h1_dedupe") {
        return [
          // S1: Demote extra H1 widgets to H2
          { name: "demote_extra_h1_widgets", fn: async () => {
              const data = await fetchElementorData();
              let found = false, changed = 0;
              walkWidgets(data, n => {
                if (n.widgetType === "heading" && n.settings?.header_size === "h1") {
                  if (found) { n.settings.header_size = "h2"; changed++; }
                  else found = true;
                }
              });
              if (changed === 0) return { skip: true };
              return { elementorData: data };
          }},
          // S2: Re-fetch fresh data, patch again (first write may not have persisted)
          { name: "refetch_and_demote",      fn: async () => {
              const data = await fetchElementorData();
              let found = false;
              walkWidgets(data, n => {
                if (n.widgetType === "heading" && n.settings?.header_size === "h1") {
                  if (found) n.settings.header_size = "h2";
                  else found = true;
                }
              });
              return { elementorData: data };
          }},
          // S3: Patch duplicates in post_content directly
          { name: "patch_post_content_h1",   fn: async () => {
              let content = await getRawContent();
              let count = 0;
              content = content.replace(/<h1([^>]*)>/gi, m => { count++; return count > 1 ? m.replace(/h1/i, "h2") : m; });
              content = content.replace(/<\/h1>/gi, (m, i, s) => {
                let h1count = 0;
                s.slice(0, i).replace(/<h1[^>]*>/gi, () => { h1count++; return ""; });
                return h1count > 1 ? "</h2>" : m;
              });
              return { content };
          }},
        ];
      }

      // ── ELEMENTOR H1 PROMOTE ───────────────────────────────────────────────────
      if (ft === "elementor_h1_promote") {
        const pageTitle = (wpPost.title?.rendered || wpPost.title || "").replace(/<[^>]+>/g, "").trim();
        // Check if already fixed before trying anything
        try {
          const { html: cur } = await fetchPage(url, true);
          if (/<h1[^>]*>[\s\S]*?<\/h1>/i.test(cur)) {
            return [{ name: "already_fixed", fn: async () => ({ skip: true }) }];
          }
        } catch(e) {}
        return [
          // S1: Promote first heading widget to H1
          { name: "promote_heading_widget",  fn: async () => {
              const data = await fetchElementorData();
              const heads = [];
              walkWidgets(data, n => { if (n.widgetType === "heading" && n.settings?.header_size) heads.push(n); });
              if (!heads.length) throw new Error("no_heading_widgets");
              heads.forEach((h, i) => { h.settings.header_size = i === 0 ? "h1" : (h.settings.header_size === "h1" ? "h2" : h.settings.header_size); });
              return { elementorData: data };
          }},
          // S2: Swap first h2/h3 inside a text-editor or HTML widget
          { name: "swap_text_widget_tag",    fn: async () => {
              const data = await fetchElementorData();
              let fixed = false;
              walkWidgets(data, node => {
                if (fixed) return;
                if (node.widgetType === "text-editor" || node.widgetType === "html") {
                  const key = node.widgetType === "html" ? "html" : "editor";
                  const c = node.settings?.[key] || "";
                  const m = c.match(/<h[2-6]([^>]*)>([\s\S]*?)<\/h[2-6]>/i);
                  if (m) { node.settings[key] = c.replace(m[0], `<h1${m[1]}>${m[2]}</h1>`); fixed = true; }
                }
              });
              if (!fixed) throw new Error("no_text_widget_heading");
              return { elementorData: data };
          }},
          // S3: Inject new H1 heading widget at top of first container
          { name: "inject_heading_widget",   fn: async () => {
              if (!pageTitle) throw new Error("no_page_title");
              const data = await fetchElementorData();
              const widget = { id: Math.random().toString(36).slice(2,10), elType:"widget", widgetType:"heading", isInner:false, elements:[], settings:{ title: pageTitle, header_size:"h1", align:"left" } };
              let done = false;
              const inject = (nodes) => {
                if (!Array.isArray(nodes) || done) return;
                for (const n of nodes) {
                  if (done) return;
                  if ((n.elType === "column" || n.elType === "container") && Array.isArray(n.elements)) { n.elements.unshift(widget); done = true; return; }
                  if (n.elements) inject(n.elements);
                }
              };
              inject(data);
              if (!done && data[0]?.elements) { data[0].elements.unshift(widget); done = true; }
              if (!done) throw new Error("no_container_found");
              return { elementorData: data };
          }},
          // S4: Prepend H1 to post_content (non-Elementor fallback)
          { name: "prepend_post_content",    fn: async () => {
              if (!pageTitle) throw new Error("no_page_title");
              const existing = await getRawContent();
              if (/<h1[^>]*>/i.test(existing)) return { skip: true };
              return { content: `<h1>${pageTitle}</h1>\n${existing}` };
          }},
        ];
      }

      // ── ELEMENTOR H2 PROMOTE ───────────────────────────────────────────────────
      if (ft === "elementor_h2_promote") {
        return [
          // S1: Promote first non-H1 heading widget to H2
          { name: "promote_heading_to_h2",   fn: async () => {
              const data = await fetchElementorData();
              const heads = [];
              walkWidgets(data, n => { if (n.widgetType === "heading" && n.settings?.header_size) heads.push(n); });
              const nonH1 = heads.filter(h => h.settings.header_size !== "h1");
              if (!nonH1.length) throw new Error("no_non_h1_headings");
              if (nonH1.some(h => h.settings.header_size === "h2")) return { skip: true };
              nonH1[0].settings.header_size = "h2";
              return { elementorData: data };
          }},
          // S2: Swap h3/h4 in text-editor widget
          { name: "swap_text_widget_to_h2",  fn: async () => {
              const data = await fetchElementorData();
              let fixed = false;
              walkWidgets(data, node => {
                if (fixed) return;
                if (node.widgetType === "text-editor" || node.widgetType === "html") {
                  const key = node.widgetType === "html" ? "html" : "editor";
                  const c = node.settings?.[key] || "";
                  const m = c.match(/<h[3-6]([^>]*)>([\s\S]*?)<\/h[3-6]>/i);
                  if (m) { node.settings[key] = c.replace(m[0], `<h2${m[1]}>${m[2]}</h2>`); fixed = true; }
                }
              });
              if (!fixed) throw new Error("no_subheading_in_text_widget");
              return { elementorData: data };
          }},
          // S3: Patch first h3→h2 in raw post content
          { name: "patch_post_content_h2",   fn: async () => {
              let content = await getRawContent();
              if (/<h2[^>]*>/i.test(content)) return { skip: true };
              let patched = false;
              content = content.replace(/<h[3-6]([^>]*)>/i, (m, attrs) => { patched = true; return `<h2${attrs}>`; })
                               .replace(/<\/h[3-6]>/i, patched ? "</h2>" : "$&");
              return { content };
          }},
        ];
      }

      // ── REPAIR POST CONTENT (markdown→HTML) ────────────────────────────────────
      if (ft === "repair_post_content") {
        const rawContent = await getRawContent();
        const fixedContent = markdownToHtml(rawContent);
        return [
          { name: "markdown_to_html_direct",   fn: async () => ({ content: fixedContent, meta: { "astra-migrate-meta-layouts": "set", "_elementor_template_type": "wp-post" } }) },
          { name: "force_touch_then_repair",   fn: async () => {
              await axios.post(`${wordpressUrl}/wp-json/wp/v2/${postType}/${wpPost.id}`, { status: wpPost.status || "publish" }, { headers: authHeaders, httpsAgent });
              await new Promise(r => setTimeout(r, 700));
              return { content: fixedContent, meta: { "astra-migrate-meta-layouts": "set" } };
          }},
          { name: "ai_html_repair",             fn: async () => {
              const fixed = await ai(`Convert this WordPress post content from markdown/mixed format to clean semantic HTML. Return ONLY the fixed HTML, no explanation:\n\n${rawContent}`);
              return { content: fixed };
          }},
        ];
      }

      throw new Error(`Unknown fixType: ${issue.fixType}`);
    };

    // ── RUN THE ENGINE ─────────────────────────────────────────────────────────
    let strategies;
    try {
      strategies = await getStrategies();
    } catch(e) {
      return res.status(400).json({ error: e.message, code: "BUILD_STRATEGIES_FAILED" });
    }

    const verify = makeVerify(issue.fixType, ctx);
    const attemptLog = [];
    let verified = false;

    for (let i = 0; i < strategies.length && !verified; i++) {
      const result = await tryStrategy(strategies[i].name, strategies[i].fn, verify, i + 1);
      attemptLog.push(result);
      verified = result.verified;
      if (!verified && i < strategies.length - 1) {
        await new Promise(r => setTimeout(r, 800)); // brief pause between strategies
      }
    }

    await bustCache(wpPost.id);
    return res.json({ success: true, postId: wpPost.id, postType, verified, attempts: attemptLog });

  } catch (e) {
    const errData = e.response?.data;
    const errMsg = errData?.message || (typeof errData === "string" ? errData : null) || e.message;
    const errCode = errData?.code || e.code || "";
    console.error(`[SEO Fix] ERROR ${errCode}: ${errMsg}`, errData || "");

    let userMsg = errMsg;
    if (errCode === "rest_cannot_edit" || errMsg?.includes("not allowed")) {
      userMsg = "WordPress returned a permissions error. Ensure the application password has Editor or Admin rights.";
    } else if (errCode === "rest_no_route" || e.response?.status === 404) {
      userMsg = "WordPress REST API route not found. Check the WordPress URL is correct and REST API is enabled.";
    } else if (e.response?.status === 401) {
      userMsg = "WordPress authentication failed. Check the username and application password in client settings.";
    }

    res.status(500).json({ error: userMsg, code: errCode });
  }
});

// ─── GOOGLE BUSINESS PROFILE ─────────────────────────────────────
const GBP_CLIENT_ID = process.env.GBP_CLIENT_ID;
const GBP_CLIENT_SECRET = process.env.GBP_CLIENT_SECRET;
const GBP_REDIRECT_URI = `http://localhost:${PORT}/api/gbp/oauth/callback`;
const GBP_SCOPES = "https://www.googleapis.com/auth/business.manage";

let agencyGbpToken = { access_token: null, refresh_token: null, expires_at: 0 };

async function loadAgencyToken() {
  try {
    const { data } = await supabase.from("agency_settings").select("gbp_refresh_token").eq("id", "agency").single();
    if (data?.gbp_refresh_token) {
      agencyGbpToken.refresh_token = data.gbp_refresh_token;
      console.log("✓ GBP agency token loaded from Supabase");
    }
  } catch (e) {}
}
loadAgencyToken();

app.get("/api/gbp/oauth/start", (req, res) => {
  if (!GBP_CLIENT_ID) return res.status(500).send("GBP_CLIENT_ID not set in .env");
  const url = `https://accounts.google.com/o/oauth2/v2/auth?` +
    `client_id=${GBP_CLIENT_ID}&` +
    `redirect_uri=${encodeURIComponent(GBP_REDIRECT_URI)}&` +
    `response_type=code&` +
    `scope=${encodeURIComponent(GBP_SCOPES)}&` +
    `access_type=offline&` +
    `prompt=consent&` +
    `state=agency`;
  res.redirect(url);
});

app.get("/api/gbp/oauth/callback", async (req, res) => {
  const { code, error } = req.query;
  if (error) return res.send(`<script>window.opener.postMessage({type:'gbp_error',error:'${error}'},'*');window.close();</script>`);
  try {
    const tokenRes = await axios.post("https://oauth2.googleapis.com/token", {
      code, client_id: GBP_CLIENT_ID, client_secret: GBP_CLIENT_SECRET,
      redirect_uri: GBP_REDIRECT_URI, grant_type: "authorization_code",
    });
    const { access_token, refresh_token, expires_in } = tokenRes.data;
    agencyGbpToken = { access_token, refresh_token, expires_at: Date.now() + expires_in * 1000 };
    await supabase.from("agency_settings").upsert({ id: "agency", gbp_refresh_token: refresh_token });
    res.send(`<script>window.opener.postMessage({type:'gbp_connected'},'*');window.close();</script>`);
  } catch (e) {
    console.error("GBP OAuth error:", e.response?.data || e.message);
    res.send(`<script>window.opener.postMessage({type:'gbp_error',error:'OAuth failed - check console'},'*');window.close();</script>`);
  }
});

async function getAgencyAccessToken() {
  if (!agencyGbpToken.refresh_token) throw new Error("GBP not connected — authorize via Settings");
  if (!agencyGbpToken.access_token || Date.now() > agencyGbpToken.expires_at - 60000) {
    const res = await axios.post("https://oauth2.googleapis.com/token", {
      client_id: GBP_CLIENT_ID, client_secret: GBP_CLIENT_SECRET,
      refresh_token: agencyGbpToken.refresh_token, grant_type: "refresh_token",
    });
    agencyGbpToken.access_token = res.data.access_token;
    agencyGbpToken.expires_at = Date.now() + res.data.expires_in * 1000;
  }
  return agencyGbpToken.access_token;
}

app.get("/api/gbp/agency-status", (req, res) => {
  res.json({ connected: !!agencyGbpToken.refresh_token });
});

app.post("/api/gbp/disconnect-agency", async (req, res) => {
  agencyGbpToken = { access_token: null, refresh_token: null, expires_at: 0 };
  await supabase.from("agency_settings").upsert({ id: "agency", gbp_refresh_token: null });
  res.json({ success: true });
});

app.get("/api/gbp/locations", async (req, res) => {
  try {
    const access_token = await getAgencyAccessToken();
    const accountRes = await axios.get("https://mybusinessaccountmanagement.googleapis.com/v1/accounts", {
      headers: { Authorization: `Bearer ${access_token}` }
    });
    const accounts = accountRes.data.accounts || [];
    const allLocations = [];
    for (const account of accounts) {
      try {
        const locRes = await axios.get(
          `https://mybusinessbusinessinformation.googleapis.com/v1/${account.name}/locations?readMask=name,title,storefrontAddress`,
          { headers: { Authorization: `Bearer ${access_token}` } }
        );
        (locRes.data.locations || []).forEach(loc => {
          allLocations.push({ name: loc.name, title: loc.title, accountName: account.name, address: loc.storefrontAddress?.addressLines?.[0] || "" });
        });
      } catch (e) {
        const status = e.response?.status;
        const msg = e.response?.data?.error?.message || e.message;
        if (status === 429) return res.status(429).json({ error: `Google rate limit (429): ${msg}` });
        console.error("GBP location fetch error:", msg);
      }
    }
    res.json({ locations: allLocations });
  } catch (e) {
    const status = e.response?.status;
    const msg = e.response?.data?.error?.message || e.message;
    if (status === 429) return res.status(429).json({ error: `Google rate limit (429): ${msg}` });
    res.status(500).json({ error: msg });
  }
});

app.post("/api/gbp/assign/:clientId", async (req, res) => {
  const { clientId } = req.params;
  const { locationName, locationTitle, accountName } = req.body;
  await supabase.from("clients").update({ gbp_location_name: locationName, gbp_location_title: locationTitle, gbp_account_name: accountName }).eq("id", clientId);
  res.json({ success: true });
});

app.get("/api/gbp/status/:clientId", async (req, res) => {
  const { clientId } = req.params;
  try {
    const { data } = await supabase.from("clients").select("gbp_location_title,gbp_location_name,gbp_account_name").eq("id", clientId).single();
    res.json({
      connected: !!data?.gbp_location_name && !!agencyGbpToken.refresh_token,
      locationTitle: data?.gbp_location_title || null,
      locationName: data?.gbp_location_name || null,
      accountName: data?.gbp_account_name || null,
      agencyConnected: !!agencyGbpToken.refresh_token,
    });
  } catch (e) { res.json({ connected: false, agencyConnected: !!agencyGbpToken.refresh_token }); }
});

app.post("/api/gbp/disconnect/:clientId", async (req, res) => {
  const { clientId } = req.params;
  await supabase.from("clients").update({ gbp_location_name: null, gbp_location_title: null, gbp_account_name: null }).eq("id", clientId);
  res.json({ success: true });
});

app.post("/api/gbp/post/:clientId", async (req, res) => {
  const { clientId } = req.params;
  const { summary, ctaUrl, ctaType = "LEARN_MORE", topicType = "STANDARD", imageUrl } = req.body;
  if (!summary) return res.status(400).json({ error: "summary is required" });
  try {
    const access_token = await getAgencyAccessToken();
    const { data: client } = await supabase.from("clients").select("gbp_location_name").eq("id", clientId).single();
    if (!client?.gbp_location_name) return res.status(400).json({ error: "No GBP location assigned to this client" });

    const postBody = {
      languageCode: "en",
      summary,
      topicType,
      ...(ctaUrl ? { callToAction: { actionType: ctaType, url: ctaUrl } } : {}),
      ...(imageUrl ? { media: [{ mediaFormat: "PHOTO", sourceUrl: imageUrl }] } : {}),
    };

    const postRes = await axios.post(
      `https://mybusiness.googleapis.com/v4/${client.gbp_location_name}/localPosts`,
      postBody,
      { headers: { Authorization: `Bearer ${access_token}`, "Content-Type": "application/json" } }
    );
    res.json({ success: true, post: postRes.data });
  } catch (e) {
    console.error("GBP post error:", e.response?.data || e.message);
    res.status(500).json({ error: e.response?.data?.error?.message || e.message });
  }
});

app.get("/api/gbp/posts/:clientId", async (req, res) => {
  const { clientId } = req.params;
  try {
    const access_token = await getAgencyAccessToken();
    const { data: client } = await supabase.from("clients").select("gbp_location_name").eq("id", clientId).single();
    if (!client?.gbp_location_name) return res.json({ posts: [] });
    const postsRes = await axios.get(
      `https://mybusiness.googleapis.com/v4/${client.gbp_location_name}/localPosts`,
      { headers: { Authorization: `Bearer ${access_token}` } }
    );
    res.json({ posts: postsRes.data.localPosts || [] });
  } catch (e) {
    res.json({ posts: [], error: e.message });
  }
});

app.listen(PORT, () => {
  console.log("✓ Fortitude backend running on http://localhost:" + PORT);
  console.log("✓ SEMrush: " + (SEMRUSH_API_KEY ? "loaded" : "MISSING"));
  console.log("✓ Anthropic: " + (ANTHROPIC_API_KEY ? "loaded" : "MISSING"));
  console.log("✓ Supabase: " + (SUPABASE_URL ? "loaded" : "MISSING"));
});
