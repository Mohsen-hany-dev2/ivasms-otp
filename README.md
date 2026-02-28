# IVASMS OTP Bot

بوت تيليجرام لإدارة حسابات IVASMS، جلب الرسائل/الأكواد، الإرسال للجروبات، ولوحة تحكم كاملة.

## المتطلبات
- Python 3.10+
- `pip install -r requirements.txt`

## الإعداد السريع
1. أنشئ بيئة:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
2. جهز ملف البيئة:
```bash
cp .env.example .env
```
3. أهم متغير مطلوب للتشغيل:
```env
TELEGRAM_BOT_TOKEN=123456789:YOUR_BOT_TOKEN
```

## التشغيل
### تشغيل كامل (الموصى به)
يشغل المرسل + لوحة التحكم + البوتات الفرعية (إن وجدت):
```bash
python main.py
```

### تشغيل منفصل
- المرسل فقط:
```bash
python bot.py
```
- لوحة التحكم فقط:
```bash
python panel_bot.py
```
- CLI الإداري:
```bash
python cli.py
```

## هيكل المشروع
```text
.
├── app/
│   ├── paths.py          # المسارات الموحدة
│   └── storage.py        # طبقة التخزين SQLite + fallback
├── apps/
│   ├── sender_bot.py     # منطق جلب/إرسال الرسائل
│   ├── panel_bot.py      # لوحة التحكم بالأزرار
│   └── admin_cli.py      # أوامر CLI الإدارية
├── bot.py                # Entry point للمرسل
├── panel_bot.py          # Entry point للوحة
├── cli.py                # Entry point للـ CLI
├── main.py               # Supervisor لتشغيل كل العمليات
├── data/                 # التخزين الأساسي (SQLite)
├── logs/                 # ملفات اللوج وقت التشغيل
├── exports/              # ملفات التصدير المؤقتة
└── daily_messages/       # بيانات يومية runtime
```

## التخزين
- التخزين الأساسي في:
  - `data/storage.db`
- للبوتات الفرعية (private namespace) يتم إنشاء:
  - `data/<namespace>/storage.db`
- أغلب البيانات (حسابات، جروبات، runtime config، ranges...) تُحفظ داخل DB.

## الصلاحيات
- الأدمن الرئيسي الافتراضي:
  - `7011309417`
- يمكن إضافة أدمنات من داخل لوحة التحكم.

## ملاحظات مهمة
- المشروع يستخدم `.gitignore` لمنع رفع ملفات runtime الحساسة/المؤقتة (`logs`, `exports`, DB, cache).
- لا ترفع `.env` أو أي توكنات إلى GitHub.
- عند تغيير إعدادات runtime من اللوحة، يتم طلب إعادة تشغيل/تحديث تلقائيًا حسب نوع الإعداد.

## نشر Fly.io
- ملف النشر الحالي: `fly.new.toml`
- Dockerfile: `Dockerfile.fly`

## أوامر صيانة مفيدة
```bash
# فحص سلامة ملفات بايثون
python -m py_compile main.py apps/panel_bot.py apps/sender_bot.py apps/admin_cli.py

# تشغيل دورة واحدة للمرسل (إن كانت مدعومة بالخيارات الحالية)
python bot.py --once
```
