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
