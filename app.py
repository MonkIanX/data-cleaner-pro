"""
Advanced Data Refinery - OpenRefine升级版
一个比OpenRefine更先进的数据清洗工具，支持：
1. 智能字段类型识别
2. 多格式日期清洗（含歧义检测）
3. 数值范围异常检测（保留原始值）
4. 可视化异常面板
5. 结构化清洗日志（JSON导出）
"""

import streamlit as st
import pandas as pd
import numpy as np
import json
import io
from datetime import datetime
import re
from pathlib import Path


# ========== Page Config ==========
st.set_page_config(
    page_title="Advanced Data Refinery",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== Custom CSS ==========
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    .stApp {
        background: linear-gradient(135deg, #f8fafc 0%, #e0f2fe 50%, #f8fafc 100%);
        font-family: 'Inter', sans-serif;
    }

    .hero-banner {
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 50%, #2563eb 100%);
        padding: 2rem;
        border-radius: 16px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 10px 25px -5px rgba(59, 130, 246, 0.4);
    }

    .hero-title {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }

    .hero-subtitle {
        font-size: 1.1rem;
        opacity: 0.9;
    }

    .stat-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #3b82f6;
    }

    .stat-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1e293b;
    }

    .stat-label {
        font-size: 0.875rem;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    .issue-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 0.5rem;
    }

    .issue-out_of_range { background-color: #fef2f2; color: #dc2626; border: 1px solid #fecaca; }
    .issue-invalid_format { background-color: #fff7ed; color: #ea580c; border: 1px solid #fed7aa; }
    .issue-ambiguous_date { background-color: #fefce8; color: #ca8a04; border: 1px solid #fef08a; }
    .issue-not_numeric { background-color: #faf5ff; color: #9333ea; border: 1px solid #e9d5ff; }
    .issue-invalid_email { background-color: #eff6ff; color: #2563eb; border: 1px solid #bfdbfe; }
    .issue-placeholder_value { background-color: #f8fafc; color: #475569; border: 1px solid #cbd5e1; }

    .anomaly-card {
        background: white;
        border-left: 4px solid #f59e0b;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.75rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }

    .code-block {
        background: #1e293b;
        color: #10b981;
        padding: 1rem;
        border-radius: 8px;
        font-family: 'Courier New', monospace;
        font-size: 0.875rem;
        overflow-x: auto;
    }
</style>
""", unsafe_allow_html=True)


# ========== Core Functions ==========

class FieldTypeDetector:
    """智能字段类型检测引擎"""

    # FIX 1: 移除死代码，合并邮箱检测逻辑，顺序调整确保每条路径都能跑到
    @staticmethod
    def detect_type(series):
        """自动检测字段类型"""
        non_null = series.dropna()
        if len(non_null) == 0:
            return 'text'

        # 数字检测
        numeric_count = pd.to_numeric(non_null, errors='coerce').notna().sum()
        if numeric_count / len(non_null) > 0.8:
            return 'number'

        # 日期检测
        date_patterns = [
            r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}$',
            r'^\d{1,2}[-/]\d{1,2}[-/]\d{4}$',
            r'^\d{4}\.\d{1,2}\.\d{1,2}$'
        ]
        date_count = sum(
            non_null.astype(str).str.match(pattern).sum()
            for pattern in date_patterns
        )
        if date_count / len(non_null) > 0.7:
            return 'date'

        # FIX 1: 邮箱检测合并为一处，列名优先，再看内容
        # 列名包含 email/mail 关键词，直接认定
        col_name = series.name.lower() if series.name else ''
        if 'email' in col_name or 'mail' in col_name:
            return 'email'

        # 内容像邮箱（兼容 # 号替代 @ 的脏数据）
        email_like_pattern = r'[a-zA-Z0-9._%+-]+[@#][a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        try:
            email_hits = non_null.astype(str).str.match(email_like_pattern).sum()
            if email_hits / len(non_null) > 0.6:
                return 'email'
        except Exception:
            pass

        # 布尔检测
        bool_values = ['true', 'false', 'yes', 'no', '1', '0', 'y', 'n', 't', 'f']
        bool_count = non_null.astype(str).str.lower().isin(bool_values).sum()
        if bool_count / len(non_null) > 0.8:
            return 'boolean'

        return 'text'


class DataCleaner:
    """高级数据清洗引擎"""

    def __init__(self):
        self.cleaning_log = []
        self.rules = {
            'age': {'min': 0, 'max': 120},
            'salary': {'min': 0, 'max': 10000000},
            'price': {'min': 0, 'max': 1000000},
            'quantity': {'min': 0, 'max': 100000}
        }

    def add_log(self, row, col, raw, cleaned, issue, hint=None, rule=None):
        self.cleaning_log.append({
            'row': row + 1,
            'column': col,
            'raw': str(raw),
            'cleaned': str(cleaned) if cleaned is not None else None,
            'issue': issue,
            'rule': rule,
            'hint': hint
        })

    def clean_numeric(self, value, row_idx, col_name):
        if pd.isna(value) or str(value).strip() == "":
            return None

        raw_str = str(value).strip()
        str_val = raw_str.lower()

        null_keywords = ['unknown', 'not a number', 'not_a_number', 'nan', 'n/a', '?', 'none', 'null', '-', 'undefined']
        if str_val in null_keywords:
            self.add_log(row_idx, col_name, raw_str, None, 'placeholder_value', f'识别为无效占位符: {raw_str}')
            return None

        temp_val = str_val.replace('$', '').replace('￥', '').replace(',', '')
        clean_num_match = re.search(r'[-+]?\d*\.?\d+', temp_val)

        try:
            if not clean_num_match:
                raise ValueError
            num = float(clean_num_match.group())
        except (ValueError, TypeError):
            self.add_log(row_idx, col_name, raw_str, None, 'not_numeric', '无法解析为数值')
            return None

        col_lower = col_name.lower()
        for rule_key, rule_val in self.rules.items():
            if rule_key in col_lower:
                if 'min' in rule_val and num < rule_val['min']:
                    self.add_log(row_idx, col_name, raw_str, None, 'out_of_range',
                                 f'数值 {num} 低于最小值 {rule_val["min"]}', rule=f'{rule_key}: min={rule_val["min"]}')
                    return None
                if 'max' in rule_val and num > rule_val['max']:
                    self.add_log(row_idx, col_name, raw_str, None, 'out_of_range',
                                 f'数值 {num} 超出最大值 {rule_val["max"]}', rule=f'{rule_key}: max={rule_val["max"]}')
                    return None
        return num

    # FIX 2: 日期清洗支持更多格式，不再对无法识别的格式静默放行
    @staticmethod
    def _try_parse_date(str_val):
        """
        尝试将字符串解析为标准日期 YYYY-MM-DD。
        支持格式：
          YYYY-MM-DD / YYYY/MM/DD / YYYY.MM.DD
          DD-MM-YYYY / DD/MM/YYYY
          MM-DD-YYYY / MM/DD/YYYY  （当 MM <= 12 且 DD > 12 时推断）
        返回 (标准化日期字符串 or None, 是否有歧义)
        """
        s = str_val.strip().lower()

        # YYYY-MM-DD 系列
        m = re.match(r'^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$', s)
        if m:
            y, mo, d = m.groups()
            try:
                datetime(int(y), int(mo), int(d))
                return f"{y}-{mo.zfill(2)}-{d.zfill(2)}", False
            except ValueError:
                return None, False

        # DD-MM-YYYY 或 MM-DD-YYYY 系列（歧义检测）
        m = re.match(r'^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})$', s)
        if m:
            a, b, y = m.groups()
            a, b = int(a), int(b)
            # 只有一种合法解释
            a_valid = 1 <= a <= 12
            b_valid = 1 <= b <= 12
            if a > 12 and b_valid:          # 只能是 DD-MM-YYYY
                return f"{y}-{str(b).zfill(2)}-{str(a).zfill(2)}", False
            elif b > 12 and a_valid:        # 只能是 MM-DD-YYYY
                return f"{y}-{str(a).zfill(2)}-{str(b).zfill(2)}", False
            elif a_valid and b_valid:       # 两种都合法 → 歧义
                return f"{y}-{str(a).zfill(2)}-{str(b).zfill(2)}", True
            else:
                return None, False          # 两种都非法

        return None, False  # 无法识别的格式

    def clean_date(self, value, row_idx, col_name):
        if pd.isna(value):
            return value
        str_val = str(value).strip()
        result, ambiguous = self._try_parse_date(str_val)

        if result is None:
            self.add_log(row_idx, col_name, str_val, None, 'invalid_format', f'无法识别的日期格式: {str_val}')
            return None  # FIX 2: 不再静默返回原始值，记录日志并返回 None

        if ambiguous:
            self.add_log(row_idx, col_name, str_val, result, 'ambiguous_date',
                         f'{str_val} 可解释为 DD/MM 或 MM/DD，已按 MM/DD 处理，请人工核查')
        return result

    def clean_email(self, value, row_idx, col_name):
        if pd.isna(value):
            return value
        email = str(value).strip().lower().replace('#', '@')
        # 清洗后验证格式，不合法的记日志并返回 None
        valid_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(valid_pattern, email):
            self.add_log(row_idx, col_name, str(value).strip(), None, 'invalid_email',
                         f'清洗后仍不是合法邮箱: {email}')
            return None
        return email

    def clean_dataframe(self, df, field_types):
        """执行清洗管道"""
        self.cleaning_log = []
        df_cleaned = df.copy()
        df_original = df.copy()

        for col in df.columns:
            if col not in field_types:
                continue

            f_type = field_types[col]

            if f_type == 'number':
                temp_list = [self.clean_numeric(val, idx, col) for idx, val in enumerate(df[col])]
                df_cleaned[col] = pd.to_numeric(temp_list, errors='coerce')

            elif f_type == 'email':
                df_cleaned[col] = [self.clean_email(val, idx, col) for idx, val in enumerate(df[col])]

            elif f_type == 'date':
                df_cleaned[col] = [self.clean_date(val, idx, col) for idx, val in enumerate(df[col])]

        # 兜底：再次强制转换所有数值列
        for col, ftype in field_types.items():
            if ftype == 'number' and col in df_cleaned.columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')

        return df_cleaned, df_original, self.cleaning_log


# ========== Session State ==========
for key, default in [
    ('data', None), ('data_cleaned', None), ('data_original', None),
    ('field_types', {}), ('cleaning_log', []), ('show_original', False)
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ========== Main App ==========

def main():
    st.markdown("""
        <div class="hero-banner">
            <div class="hero-title">🔬 Advanced Data Refinery</div>
            <div class="hero-subtitle">
                Smart Data Cleaning Engine · Anomaly Detection · Structured Logging
            </div>
        </div>
    """, unsafe_allow_html=True)

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")

        uploaded_file = st.file_uploader(
            "Upload Dataset",
            type=['csv', 'xlsx', 'xls'],
            help="支持CSV和Excel格式"
        )

        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)

                st.session_state.data = df
                st.success(f"✅ Loaded {len(df)} rows × {len(df.columns)} columns")

                if st.button("🤖 Auto-Detect Field Types", use_container_width=True):
                    detector = FieldTypeDetector()
                    types = {col: detector.detect_type(df[col]) for col in df.columns}
                    st.session_state.field_types = types
                    st.success("✅ Field types detected!")
                    st.rerun()

            except Exception as e:
                st.error(f"Error loading file: {e}")

        st.markdown("---")

        if st.button("📊 Load Sample Data", use_container_width=True):
            sample_data = pd.DataFrame({
                'id': [1, 2, 3, 4, 5, 6],
                'name': ['Zhang San', 'Li Si', 'Wang Wu', 'Zhao Liu', 'Qian Qi', 'Sun Ba'],
                'age': [25, 30, 150, 'invalid', 28, -5],
                'email': ['zhang#example.com', 'lisi@example', 'wang@test.com', 'zhao@company.cn', 'not-an-email', 'qian@tech.io'],
                'date': ['2024-01-15', '01/02/2024', '2024.03.20', '2024-04-01', 'not-a-date', '13/02/2024'],
                'salary': [5000, 6000, -1000, 8000, 7500, '$9,500']
            })
            st.session_state.data = sample_data
            detector = FieldTypeDetector()
            st.session_state.field_types = {
                col: detector.detect_type(sample_data[col])
                for col in sample_data.columns if col != 'id'
            }
            st.success("✅ Sample data loaded!")
            st.rerun()

        st.markdown("---")
        st.markdown("### About")
        st.info(
            "**Advanced Data Refinery v2.1**\n\n"
            "修复内容:\n"
            "• 邮箱检测死代码修复\n"
            "• 日期多格式支持 + 歧义检测\n"
            "• 无效日期不再静默放行\n"
            "• 异常日志 hint 更详细"
        )

    if st.session_state.data is None:
        st.info("👈 请从侧边栏上传数据或加载示例数据")
        return

    df = st.session_state.data

    # Stats Bar
    col1, col2, col3, col4 = st.columns(4)
    issues = len(st.session_state.cleaning_log)

    with col1:
        st.markdown(f'<div class="stat-card"><div class="stat-label">Total Records</div><div class="stat-value">{len(df)}</div></div>', unsafe_allow_html=True)
    with col2:
        st.markdown(f'<div class="stat-card"><div class="stat-label">Fields Detected</div><div class="stat-value">{len(st.session_state.field_types)}</div></div>', unsafe_allow_html=True)
    with col3:
        st.markdown(f'<div class="stat-card" style="border-left-color:#f59e0b"><div class="stat-label">Issues Found</div><div class="stat-value" style="color:#f59e0b">{issues}</div></div>', unsafe_allow_html=True)
    with col4:
        denom = len(df) * max(len(st.session_state.field_types), 1)
        clean_rate = max(0, 100 - (issues / denom * 100)) if issues > 0 else 100
        st.markdown(f'<div class="stat-card" style="border-left-color:#10b981"><div class="stat-label">Clean Rate</div><div class="stat-value" style="color:#10b981">{clean_rate:.0f}%</div></div>', unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs(["📊 Data View", "⚙️ Field Types", "🚨 Anomaly Panel", "📋 Cleaning Logs"])

    with tab1:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader("Data Preview")
        with col2:
            view_mode = st.radio("View Mode", ["Cleaned", "Original"], horizontal=True)

        display_df = (
            st.session_state.data_original
            if view_mode == "Original" and st.session_state.data_original is not None
            else (st.session_state.data_cleaned if st.session_state.data_cleaned is not None else df)
        )
        st.dataframe(display_df, use_container_width=True, height=400)

        if st.session_state.data_cleaned is not None:
            csv = st.session_state.data_cleaned.to_csv(index=False)
            st.download_button(
                label="📥 Download Cleaned Data (CSV)",
                data=csv,
                file_name=f"cleaned_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

    with tab2:
        st.subheader("Auto-Detected Field Types")

        if not st.session_state.field_types:
            st.warning("⚠️ 请先在侧边栏点击 'Auto-Detect Field Types'")
        else:
            type_icons = {'text': '📝', 'number': '🔢', 'date': '📅', 'email': '📧', 'boolean': '✓'}
            cols = st.columns(3)
            for idx, (field, ftype) in enumerate(st.session_state.field_types.items()):
                with cols[idx % 3]:
                    icon = type_icons.get(ftype, '❓')
                    st.markdown(f"""
                        <div style="background:white;padding:1rem;border-radius:8px;border-left:4px solid #3b82f6;margin-bottom:1rem;">
                            <div style="font-size:1.5rem;margin-bottom:0.5rem;">{icon}</div>
                            <div style="font-weight:600;color:#1e293b;">{field}</div>
                            <div style="font-size:0.75rem;color:#64748b;text-transform:uppercase;">{ftype}</div>
                        </div>
                    """, unsafe_allow_html=True)

            st.markdown("---")
            st.subheader("⚙️ Cleaning Rules")
            st.info("为数值字段配置验证规则（可选）")

            numeric_fields = [f for f, t in st.session_state.field_types.items() if t == 'number']
            if numeric_fields:
                for field in numeric_fields:
                    with st.expander(f"🔢 {field} - Range Rules"):
                        c1, c2 = st.columns(2)
                        with c1:
                            st.number_input("Minimum value", value=0.0, key=f"min_{field}")
                        with c2:
                            st.number_input("Maximum value", value=1000000.0, key=f"max_{field}")

            if st.button("🚀 Start Data Cleaning Pipeline", type="primary", use_container_width=True):
                with st.spinner("🔄 Processing data..."):
                    cleaner = DataCleaner()
                    # 将用户自定义的规则写入 cleaner
                    for field in numeric_fields:
                        cleaner.rules[field.lower()] = {
                            'min': st.session_state.get(f"min_{field}", 0),
                            'max': st.session_state.get(f"max_{field}", 1000000)
                        }
                    df_cleaned, df_original, logs = cleaner.clean_dataframe(df, st.session_state.field_types)
                    st.session_state.data_cleaned = df_cleaned
                    st.session_state.data_original = df_original
                    st.session_state.cleaning_log = logs
                    st.success(f"✅ Cleaning complete! Found {len(logs)} issues.")
                    st.rerun()

    with tab3:
        if not st.session_state.cleaning_log:
            st.info("ℹ️ 没有检测到异常。请先运行数据清洗。")
        else:
            st.subheader("🚨 Anomaly Detection Panel")
            logs = st.session_state.cleaning_log
            issue_counts = {}
            for log in logs:
                issue_counts[log['issue']] = issue_counts.get(log['issue'], 0) + 1

            issue_labels = {
                'out_of_range': '超出范围',
                'invalid_format': '格式错误',
                'ambiguous_date': '日期歧义',
                'not_numeric': '非数值',
                'invalid_email': '邮箱格式',
                'placeholder_value': '占位符'
            }

            st.markdown("#### 📊 Issue Statistics")
            cols = st.columns(max(len(issue_counts), 1))
            for idx, (issue, count) in enumerate(issue_counts.items()):
                with cols[idx]:
                    st.markdown(f"""
                        <div style="background:white;padding:1rem;border-radius:8px;text-align:center;border:2px solid #e2e8f0;">
                            <div style="font-size:2rem;font-weight:700;color:#1e293b;">{count}</div>
                            <div style="font-size:0.75rem;color:#64748b;">{issue_labels.get(issue, issue)}</div>
                        </div>
                    """, unsafe_allow_html=True)

            st.markdown("---")
            c1, c2, c3 = st.columns([2, 2, 1])
            with c1:
                filter_column = st.selectbox("Filter by Column", ["All"] + list(set(l['column'] for l in logs)))
            with c2:
                filter_issue = st.selectbox("Filter by Issue Type", ["All"] + list(issue_counts.keys()))
            with c3:
                json_data = json.dumps(logs, indent=2, ensure_ascii=False)
                st.download_button("📥 Export JSON", data=json_data,
                                   file_name=f"anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                                   mime="application/json", use_container_width=True)

            filtered_logs = [
                l for l in logs
                if (filter_column == "All" or l['column'] == filter_column)
                and (filter_issue == "All" or l['issue'] == filter_issue)
            ]

            st.markdown(f"#### Found {len(filtered_logs)} anomalies")

            for log in filtered_logs:
                import html as html_lib
                issue_class = f"issue-{log['issue']}"
                rule_html = f"""
                    <div>
                        <div style="color:#64748b;font-size:0.75rem;margin-bottom:0.25rem;">Rule</div>
                        <code style="background:#dbeafe;padding:0.25rem 0.5rem;border-radius:4px;font-size:0.7rem;">{html_lib.escape(str(log['rule']))}</code>
                    </div>
                """ if log.get('rule') else ''
                hint_html = f'<div style="margin-top:0.75rem;background:#fef3c7;padding:0.5rem;border-radius:4px;font-size:0.8rem;color:#92400e;">💡 {html_lib.escape(str(log["hint"]))}</div>' if log.get('hint') else ''

                st.markdown(f"""
                    <div class="anomaly-card">
                        <div style="display:flex;justify-content:space-between;align-items:start;margin-bottom:0.75rem;">
                            <div>
                                <strong>Row {log['row']}</strong> → <strong>{log['column']}</strong>
                                <span class="issue-badge {issue_class}">{issue_labels.get(log['issue'], log['issue'])}</span>
                            </div>
                        </div>
                        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:1rem;font-size:0.875rem;">
                            <div>
                                <div style="color:#64748b;font-size:0.75rem;margin-bottom:0.25rem;">Original</div>
                                <code style="background:#f1f5f9;padding:0.25rem 0.5rem;border-radius:4px;">{log['raw']}</code>
                            </div>
                            <div>
                                <div style="color:#64748b;font-size:0.75rem;margin-bottom:0.25rem;">Cleaned</div>
                                <code style="background:#dcfce7;padding:0.25rem 0.5rem;border-radius:4px;">{log['cleaned'] if log['cleaned'] is not None else 'null'}</code>
                            </div>
                            {rule_html}
                        </div>
                        {hint_html}
                    </div>
                """, unsafe_allow_html=True)

    with tab4:
        st.subheader("📋 Structured Cleaning Logs (JSON)")
        if not st.session_state.cleaning_log:
            st.info("ℹ️ 没有清洗日志。请先运行数据清洗。")
        else:
            json_data = json.dumps(st.session_state.cleaning_log, indent=2, ensure_ascii=False)
            st.download_button("📥 下载 JSON", data=json_data,
                               file_name=f"cleaning_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                               mime="application/json")
            st.json(st.session_state.cleaning_log)

            st.markdown("---")
            st.markdown("#### 📊 Log Summary")
            summary = {
                'Total Issues': len(st.session_state.cleaning_log),
                'Affected Rows': len(set(l['row'] for l in st.session_state.cleaning_log)),
                'Affected Columns': len(set(l['column'] for l in st.session_state.cleaning_log)),
                'Issue Types': len(set(l['issue'] for l in st.session_state.cleaning_log))
            }
            cols = st.columns(4)
            for idx, (key, value) in enumerate(summary.items()):
                with cols[idx]:
                    st.metric(key, value)


if __name__ == "__main__":
    main()