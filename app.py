from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
import pandas as pd
import joblib, os, requests
from datetime import datetime, timedelta
from functools import wraps
import signal
import sys
import config
from firebase_init import init_firebase
from firebase_admin import firestore

app = Flask(__name__)
app.config['SECRET_KEY'] = config.SECRET_KEY
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=1)

# =========================================================
#               ERROR HANDLERS & TIMEOUT
# =========================================================

@app.errorhandler(404)
def not_found_error(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    print(f"Internal Server Error: {error}")
    return render_template('500.html'), 500

@app.errorhandler(Exception)
def handle_exception(error):
    print(f"Unhandled Exception: {error}")
    flash("Đã xảy ra lỗi hệ thống. Vui lòng thử lại.", "danger")
    return redirect(url_for('index'))

@app.after_request
def after_request(response):
    """Thêm headers để tránh caching issues"""
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response
# =========================================================
#               SESSION & SECURITY
# =========================================================

# ----------------- RESET SESSION ON STARTUP -----------------
@app.before_request
def clear_session_on_start():
    if request.endpoint == 'static':
        return
    if not hasattr(app, '_session_cleared'):
        session.clear()
        app._session_cleared = True

@app.before_request
def make_session_permanent():
    session.permanent = True

# ----------------- LOGIN CHECK DECORATOR -----------------
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            flash("Vui lòng đăng nhập để truy cập trang này.", "warning")
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function
# =========================================================
#               INITIALIZATION
# =========================================================

# ----------------- LOAD MODEL -----------------
MODEL_PATH = os.path.join(os.path.dirname(__file__), "data", "yield_model.pkl")
model = joblib.load(MODEL_PATH) if os.path.exists(MODEL_PATH) else None

# ----------------- INIT FIREBASE -----------------
db = None
if config.USE_FIREBASE:
    try:
        db = init_firebase()
        print("✅ Firebase initialized.")
    except Exception as e:
        db = None
        print("❌ Firebase init failed:", e)

# ----------------- HELPER PATHS -----------------
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
USERS_CSV = os.path.join(DATA_DIR, "users.csv")
SEASONS_CSV = os.path.join(DATA_DIR, "seasons.csv")
WEATHER_CSV = os.path.join(DATA_DIR, "weather_all_vn_annual_2000-2030.csv")

# =========================================================
#               CORE FUNCTIONS
# =========================================================

# ----------------- YIELD CALCULATION FUNCTION -----------------
def calculate_yield(season_data):
    """
    Tính toán năng suất tự động dựa trên:
    - Giống cây trồng (crop)
    - Diện tích (area)
    - Thời gian trồng (sow_date, harvest_date)
    - Phân bón (fertilizer)
    - Tỉnh thành (province) - ảnh hưởng thời tiết
    """
    try:
        # Base yield by crop type (tấn/ha)
        base_yields = {
            "lúa": 5.5,
            "ngô": 4.8,
            "hoa hướng dương": 2.5,
            "cà phê": 2.2,
            "cao su": 1.8,
            "chè": 3.2,
            "tiêu": 3.0,
            "điều": 1.5,
            "mía": 60.0,
            "lạc": 2.2,
            "đậu tương": 2.0
        }
        
        crop = season_data.get("crop", "").strip().lower()
        area = float(season_data.get("area", 1))
        fertilizer = season_data.get("fertilizer", "").strip().lower()
        
        # Base yield từ loại cây trồng
        base_yield = base_yields.get(crop, 4.0)
        
        # Tính thời gian sinh trưởng
        sow_date_str = season_data.get("sow_date")
        harvest_date_str = season_data.get("harvest_date")
        
        growth_days = 90  # mặc định 90 ngày
        
        if sow_date_str and harvest_date_str:
            try:
                sow_date = datetime.strptime(sow_date_str, "%Y-%m-%d")
                harvest_date = datetime.strptime(harvest_date_str, "%Y-%m-%d")
                growth_days = (harvest_date - sow_date).days
                growth_days = max(60, min(180, growth_days))
            except:
                growth_days = 90
        
        # Hệ số thời gian sinh trưởng
        if growth_days < 80:
            growth_factor = 0.7
        elif growth_days < 100:
            growth_factor = 0.9
        elif growth_days < 120:
            growth_factor = 1.0
        elif growth_days < 150:
            growth_factor = 1.1
        else:
            growth_factor = 1.2
        
        # Hệ số phân bón
        fertilizer_factors = {
            "hữu cơ": 1.2,
            "vô cơ": 1.1,
            "npk": 1.15,
            "phân chuồng": 1.18,
            "không": 0.8
        }
        
        fertilizer_factor = 1.0
        for fert_type, factor in fertilizer_factors.items():
            if fert_type in fertilizer:
                fertilizer_factor = factor
                break
        
        # Hệ số vùng miền
        region_factors = {
            "an giang": 1.3, "đồng tháp": 1.25, "long an": 1.2,
            "hà nội": 1.1, "bắc ninh": 1.05, "hưng yên": 1.05,
            "đắk lắk": 1.0, "đắk nông": 0.95, "gia lai": 0.95,
            "bắc kạn": 0.9, "cao bằng": 0.85, "hà giang": 0.85
        }
        
        province = season_data.get("province", "").strip().lower()
        region_factor = 1.0
        for region, factor in region_factors.items():
            if region in province:
                region_factor = factor
                break
        
        # Tính năng suất cuối cùng (tấn/ha)
        final_yield_per_ha = base_yield * growth_factor * fertilizer_factor * region_factor
        
        # Áp dụng cho diện tích cụ thể (tổng sản lượng)
        total_yield = final_yield_per_ha * area
        
        return round(total_yield, 2)
        
    except Exception as e:
        print(f"Lỗi tính năng suất: {e}")
        return None

# ----------------- DECISION SUPPORT FUNCTION -----------------
def generate_decision_support(season_data, predicted_yield):
    """
    Tạo dữ liệu hỗ trợ ra quyết định với báo cáo, khuyến nghị và phân tích
    """
    try:
        crop = season_data.get("crop", "").strip().lower()
        area = float(season_data.get("area", 1))
        province = season_data.get("province", "")
        fertilizer = season_data.get("fertilizer", "")
        
        # Tính toán các chỉ số
        yield_per_ha = predicted_yield / area if area > 0 else 0
        
        # Phân loại năng suất
        if yield_per_ha >= 6:
            yield_category = "Rất cao"
            yield_color = "text-green-600"
            yield_bg = "bg-green-100"
        elif yield_per_ha >= 4:
            yield_category = "Cao"
            yield_color = "text-green-500"
            yield_bg = "bg-green-50"
        elif yield_per_ha >= 2:
            yield_category = "Trung bình"
            yield_color = "text-yellow-600"
            yield_bg = "bg-yellow-50"
        else:
            yield_category = "Thấp"
            yield_color = "text-red-600"
            yield_bg = "bg-red-50"
        
        # Khuyến nghị theo loại cây trồng
        crop_recommendations = {
            "lúa": [
                "🌾 Bón thúc đợt 1: 7-10 ngày sau sạ",
                "💧 Duy trì mực nước 3-5cm trong giai đoạn đẻ nhánh",
                "🛡️ Phòng trừ sâu bệnh: đạo ôn, rầy nâu",
                "📅 Thu hoạch khi 85-90% hạt chín vàng"
            ],
            "ngô": [
                "🌱 Bón lót phân chuồng + lân trước khi gieo",
                "💦 Tưới đủ ẩm giai đoạn trỗ cờ phun râu",
                "🪲 Phòng trừ sâu đục thân, bệnh khô vằn",
                "🌽 Thu hoạch khi hạt cứng, râu chuyển nâu"
            ],
            "cà phê": [
                "🌿 Tỉa cành tạo tán sau thu hoạch",
                "💧 Tưới nước đầy đủ mùa khô",
                "🍂 Bón phân NPK cân đối theo giai đoạn",
                "☀️ Che bóng hợp lý tránh nắng gắt"
            ]
        }
        
        # Khuyến nghị chung
        general_recommendations = [
            "📊 Theo dõi thời tiết thường xuyên để điều chỉnh lịch chăm sóc",
            "🌱 Kiểm tra độ ẩm đất trước khi tưới nước",
            "🔍 Thăm đồng thường xuyên để phát hiện sâu bệnh sớm",
            "📝 Ghi chép nhật ký đồng ruộng để cải thiện vụ sau"
        ]
        
        # Cảnh báo dựa trên điều kiện
        warnings = []
        if not fertilizer or "không" in fertilizer.lower():
            warnings.append("⚠️ Chưa sử dụng phân bón - có thể ảnh hưởng năng suất")
        
        # Phân tích lợi nhuận ước tính
        crop_prices = {
            "lúa": 7000, "ngô": 6000, "cà phê": 45000, "cao su": 35000,
            "chè": 25000, "tiêu": 80000, "điều": 30000, "mía": 1000,
            "lạc": 20000, "đậu tương": 15000
        }
        
        price_per_kg = crop_prices.get(crop, 10000)
        estimated_revenue = predicted_yield * 1000 * price_per_kg
        
        # Chi phí ước tính (VND/ha)
        cost_per_ha = {
            "lúa": 15000000, "ngô": 18000000, "cà phê": 25000000,
            "cao su": 15000000, "chè": 20000000, "default": 15000000
        }
        
        cost = cost_per_ha.get(crop, cost_per_ha["default"]) * area
        estimated_profit = estimated_revenue - cost
        
        # Tạo dữ liệu biểu đồ (mẫu)
        growth_stages = [
            {"stage": "Gieo trồng", "progress": 100, "tasks": ["Làm đất", "Gieo hạt"]},
            {"stage": "Phát triển", "progress": 65, "tasks": ["Bón thúc", "Tưới nước"]},
            {"stage": "Ra hoa", "progress": 30, "tasks": ["Bón phân", "Phun thuốc"]},
            {"stage": "Thu hoạch", "progress": 0, "tasks": ["Chuẩn bị thu", "Bảo quản"]}
        ]
        
        return {
            "yield_per_ha": round(yield_per_ha, 2),
            "yield_category": yield_category,
            "yield_color": yield_color,
            "yield_bg": yield_bg,
            "crop_recommendations": crop_recommendations.get(crop, general_recommendations),
            "general_recommendations": general_recommendations,
            "warnings": warnings,
            "estimated_revenue": f"{estimated_revenue:,.0f}",
            "estimated_profit": f"{estimated_profit:,.0f}",
            "cost": f"{cost:,.0f}",
            "growth_stages": growth_stages,
            "profit_margin": round((estimated_profit / estimated_revenue * 100) if estimated_revenue > 0 else 0, 1),
            "price_per_kg": f"{price_per_kg:,.0f}"
        }
        
    except Exception as e:
        print(f"Lỗi tạo hỗ trợ quyết định: {e}")
        return None

