from __future__ import annotations

import calendar as _calendar
from datetime import date, datetime
from functools import wraps
from pathlib import Path
from uuid import uuid4
from urllib.parse import urlparse

from flask import Blueprint, abort, flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename

from .assets import cleanup_orphaned_uploads, collect_referenced_upload_urls, extract_local_upload_urls, is_local_upload_url
from .rendering import render_markdown_document
from .repository import (
    CATEGORY_LABELS,
    STATUS_LABELS,
    create_log,
    dashboard_stats,
    default_db_path,
    delete_log,
    ensure_db,
    get_log,
    list_audits,
    list_logs,
    monthly_archives,
    parse_datetime_input,
    published_dates_in_month,
    recent_audits_by_category,
    symbol_groups,
    to_datetime_local,
    to_display_datetime,
    update_log,
)

CATEGORY_SETTINGS = {
    "technical_summary": {
        "list_title": "复盘文档",
        "hero_title": "复盘文档",
        "hero_copy": "",
        "new_title": "新建文档",
        "edit_title": "编辑文档",
        "submit_label": "保存技术文档",
        "summary_label": "主题摘要",
        "summary_placeholder": "如 均线算法设计 / 缓存策略优化 / 页面交互总结",
        "content_label": "技术总结内容",
        "content_placeholder": "记录方案背景、实现要点、踩坑、取舍和后续优化方向",
        "content_help": "支持 Markdown：标题、段落、表格、图片、引用、代码块等都会在详情页按文章样式渲染。",
        "show_cover_image": True,
        "cover_image_label": "封面图链接",
        "cover_image_placeholder": "https://example.com/cover.png",
        "event_date_label": "总结日期",
        "symbol_label": "关联标的",
        "symbol_placeholder": "可选，如 600519；无则留空",
        "show_event_date": False,
        "show_published_at_input": False,
        "show_symbol": False,
        "list_endpoint": "operation_log.technical_summaries_page",
        "create_endpoint": "operation_log.create_technical_summary_page",
    },
    "trading_rules": {
        "list_title": "交易规则",
        "hero_title": "交易规则",
        "hero_copy": "",
        "new_title": "新建交易规则文档",
        "edit_title": "编辑文档",
        "submit_label": "保存",
        "summary_label": "主题摘要",
        "summary_placeholder": "如 止损策略 / 仓位管理",
        "content_label": "内容",
        "content_placeholder": "记录交易规则、执行标准、违规处理等",
        "content_help": "支持 Markdown 格式。",
        "show_cover_image": True,
        "cover_image_label": "封面图链接",
        "cover_image_placeholder": "https://example.com/cover.png",
        "event_date_label": "日期",
        "symbol_label": "关联标的",
        "symbol_placeholder": "可选，如 600519；无则留空",
        "show_event_date": False,
        "show_published_at_input": False,
        "show_symbol": False,
        "list_endpoint": "operation_log.trading_rules_page",
        "create_endpoint": "operation_log.create_trading_rule_page",
    },
    "market_strategy": {
        "list_title": "市场策略",
        "hero_title": "市场策略",
        "hero_copy": "",
        "new_title": "新建市场策略文档",
        "edit_title": "编辑文档",
        "submit_label": "保存",
        "summary_label": "主题摘要",
        "summary_placeholder": "如 板块轮动 / 趋势跟踪",
        "content_label": "内容",
        "content_placeholder": "记录市场分析框架、操作策略等",
        "content_help": "支持 Markdown 格式。",
        "show_cover_image": True,
        "cover_image_label": "封面图链接",
        "cover_image_placeholder": "https://example.com/cover.png",
        "event_date_label": "日期",
        "symbol_label": "关联标的",
        "symbol_placeholder": "可选，如 600519；无则留空",
        "show_event_date": False,
        "show_published_at_input": False,
        "show_symbol": False,
        "list_endpoint": "operation_log.market_strategy_page",
        "create_endpoint": "operation_log.create_market_strategy_page",
    },
    "trend_forecast": {
        "list_title": "趋势预测",
        "hero_title": "趋势预测",
        "hero_copy": "",
        "new_title": "新建趋势预测文档",
        "edit_title": "编辑文档",
        "submit_label": "保存",
        "summary_label": "主题摘要",
        "summary_placeholder": "如 大盘走势 / 个股研判",
        "content_label": "内容",
        "content_placeholder": "记录趋势判断依据、目标价位、时间周期等",
        "content_help": "支持 Markdown 格式。",
        "show_cover_image": True,
        "cover_image_label": "封面图链接",
        "cover_image_placeholder": "https://example.com/cover.png",
        "event_date_label": "日期",
        "symbol_label": "关联标的",
        "symbol_placeholder": "可选，如 600519；无则留空",
        "show_event_date": False,
        "show_published_at_input": False,
        "show_symbol": False,
        "list_endpoint": "operation_log.trend_forecast_page",
        "create_endpoint": "operation_log.create_trend_forecast_page",
    },
}

ALLOWED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
MAX_IMAGE_SIZE = 5 * 1024 * 1024


def create_operation_log_blueprint(root_path: str) -> Blueprint:
    blueprint = Blueprint(
        "operation_log",
        __name__,
        url_prefix="/operation-log",
        template_folder="templates",
        static_folder="static",
        static_url_path="/static",
    )
    db_path = default_db_path(root_path)
    upload_root = Path(__file__).resolve().parent / "static" / "uploads"
    ensure_db(db_path)

    def login_required(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if "username" not in session:
                return redirect(url_for("login", next=request.full_path.rstrip("?")))
            return view(*args, **kwargs)

        return wrapped

    def api_login_required(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if "username" not in session:
                return jsonify({"error": "请先登录"}), 401
            return view(*args, **kwargs)

        return wrapped

    def save_uploaded_image(image_storage) -> str:
        filename = secure_filename(image_storage.filename or "")
        extension = Path(filename).suffix.lower()
        if extension not in ALLOWED_IMAGE_EXTENSIONS:
            raise ValueError("仅支持 png、jpg、jpeg、gif、webp 图片")

        image_storage.stream.seek(0, 2)
        size = image_storage.stream.tell()
        image_storage.stream.seek(0)
        if size <= 0:
            raise ValueError("上传文件不能为空")
        if size > MAX_IMAGE_SIZE:
            raise ValueError("图片大小不能超过 5MB")

        today_dir = datetime.now().strftime("%Y%m")
        target_dir = upload_root / today_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        saved_name = f"{uuid4().hex}{extension}"
        image_path = target_dir / saved_name
        image_storage.save(image_path)
        return url_for("operation_log.static", filename=f"uploads/{today_dir}/{saved_name}")

    def normalize_form_payload(existing_log: dict[str, object] | None = None) -> dict[str, str | None]:
        category = request.form.get("category", "technical_summary").strip()
        category_config = CATEGORY_SETTINGS.get(category)
        title = request.form.get("title", "").strip()
        cover_image_url = request.form.get("cover_image_url", "").strip()
        symbol = request.form.get("symbol", "").strip().upper()
        action_summary = request.form.get("action_summary", "").strip()
        content = request.form.get("content", "").strip()
        event_date = request.form.get("event_date", "").strip()
        status = request.form.get("status", "draft").strip()
        published_at_raw = request.form.get("published_at", "")

        if category_config is None:
            raise ValueError("日志分类不合法")
        if not title:
            raise ValueError("标题不能为空")
        if not content:
            raise ValueError("日志内容不能为空")
        if category_config["show_event_date"] and not event_date:
            raise ValueError("请填写复盘日期")
        if status not in STATUS_LABELS:
            raise ValueError("日志状态不合法")
        if cover_image_url:
            if not is_local_upload_url(cover_image_url):
                parsed_cover = urlparse(cover_image_url)
                if parsed_cover.scheme not in {"http", "https"} or not parsed_cover.netloc:
                    raise ValueError("封面图链接必须是有效的 http 或 https 地址")
            elif not cover_image_url.startswith("/operation-log/static/uploads/"):
                raise ValueError("封面图链接必须是有效的 http 或 https 地址")

        published_at = parse_datetime_input(published_at_raw) if published_at_raw.strip() else None
        if not category_config["show_published_at_input"]:
            existing_published_at = str(existing_log.get("published_at") or "").strip() if existing_log else ""
            if status == "published":
                published_at = existing_published_at or parse_datetime_input(datetime.now().strftime("%Y-%m-%dT%H:%M"))
            else:
                published_at = existing_published_at if existing_published_at else None
        elif status == "published" and not published_at:
            published_at = parse_datetime_input(datetime.now().strftime("%Y-%m-%dT%H:%M"))

        if not category_config["show_symbol"]:
            symbol = ""
        if not category_config["show_cover_image"]:
            cover_image_url = ""
        if not category_config["show_event_date"]:
            if published_at:
                event_date = published_at[:10]
            else:
                event_date = date.today().isoformat()

        return {
            "category": category,
            "title": title,
            "cover_image_url": cover_image_url,
            "symbol": symbol,
            "action_summary": action_summary,
            "content": content,
            "event_date": event_date,
            "published_at": published_at,
            "status": status,
        }

    @blueprint.app_context_processor
    def inject_operation_log_helpers() -> dict[str, object]:
        return {
            "operation_log_category_labels": CATEGORY_LABELS,
            "operation_log_status_labels": STATUS_LABELS,
            "operation_log_display_datetime": to_display_datetime,
            "operation_log_category_settings": CATEGORY_SETTINGS,
        }

    def render_category_list(category: str):
        keyword = request.args.get("keyword", "").strip()
        status = request.args.get("status", "").strip()
        scope = request.args.get("scope", "active").strip() or "active"
        if scope not in {"active", "deleted", "all"}:
            scope = "active"
        config = CATEGORY_SETTINGS[category]

        today = date.today()
        cal_year = int(request.args.get("cal_year", today.year))
        cal_month = int(request.args.get("cal_month", today.month))
        # clamp to valid range
        if cal_month < 1:
            cal_month = 1
        elif cal_month > 12:
            cal_month = 12

        cal_matrix = _calendar.monthcalendar(cal_year, cal_month)
        pub_days = published_dates_in_month(db_path, cal_year, cal_month, category=category)

        # prev / next month
        if cal_month == 1:
            prev_year, prev_month = cal_year - 1, 12
        else:
            prev_year, prev_month = cal_year, cal_month - 1
        if cal_month == 12:
            next_year, next_month = cal_year + 1, 1
        else:
            next_year, next_month = cal_year, cal_month + 1

        return render_template(
            "operation_log/list.html",
            logs=list_logs(db_path, keyword=keyword, status=status, category=category, scope=scope),
            recent_actions=recent_audits_by_category(db_path, category=category),
            stats=dashboard_stats(db_path, category=category),
            archives=monthly_archives(db_path, category=category),
            symbols=symbol_groups(db_path, category=category),
            keyword=keyword,
            status=status,
            category=category,
            scope=scope,
            page_config=config,
            status_labels=STATUS_LABELS,
            cal_year=cal_year,
            cal_month=cal_month,
            cal_matrix=cal_matrix,
            pub_days=pub_days,
            prev_year=prev_year,
            prev_month=prev_month,
            next_year=next_year,
            next_month=next_month,
            today=today,
        )

    def render_form_page(page_title: str, submit_label: str, form_data: dict[str, object], is_new_form: bool = False):
        category = str(form_data.get("category") or "technical_summary")
        config = CATEGORY_SETTINGS[category]
        return render_template(
            "operation_log/form.html",
            page_title=page_title,
            submit_label=submit_label,
            form_data=form_data,
            page_config=config,
            status_labels=STATUS_LABELS,
            category_settings=CATEGORY_SETTINGS,
            category_labels=CATEGORY_LABELS,
            to_datetime_local=to_datetime_local,
            is_new_form=is_new_form,
        )

    def cleanup_removed_images(previous_log: dict[str, object], *, exclude_log_id: int | None = None) -> None:
        if str(previous_log.get("category")) != "technical_summary":
            return

        candidates = extract_local_upload_urls(
            str(previous_log.get("content") or ""),
            str(previous_log.get("cover_image_url") or ""),
        )
        if not candidates:
            return

        logs = list_logs(db_path, scope="all")
        if exclude_log_id is not None:
            logs = [log for log in logs if int(log.get("id", 0)) != exclude_log_id]
        referenced = collect_referenced_upload_urls(logs)
        cleanup_orphaned_uploads(upload_root, candidates, referenced)

    @blueprint.get("/")
    @login_required
    def list_logs_page():
        return redirect(url_for("operation_log.technical_summaries_page"))

    @blueprint.get("/technical-summaries")
    @login_required
    def technical_summaries_page():
        return render_category_list("technical_summary")

    @blueprint.get("/trading-rules")
    @login_required
    def trading_rules_page():
        return render_category_list("trading_rules")

    @blueprint.get("/market-strategy")
    @login_required
    def market_strategy_page():
        return render_category_list("market_strategy")

    @blueprint.get("/trend-forecast")
    @login_required
    def trend_forecast_page():
        return render_category_list("trend_forecast")

    @blueprint.route("/new", methods=["GET", "POST"])
    @login_required
    def create_page():
        category = request.args.get("category", "technical_summary")
        if category not in CATEGORY_SETTINGS:
            category = "technical_summary"
        form_data: dict[str, object] = {
            "category": category,
            "title": "",
            "cover_image_url": "",
            "symbol": "",
            "action_summary": "",
            "content": "",
            "event_date": "",
            "published_at": "",
            "status": "draft",
        }

        if request.method == "POST":
            form_data = request.form.to_dict()
            post_category = str(form_data.get("category") or "technical_summary")
            try:
                log_id = create_log(db_path, normalize_form_payload(), session.get("username", ""))
            except ValueError as exc:
                flash(str(exc), "error")
            else:
                flash(f"{CATEGORY_LABELS.get(post_category, '日志')}已创建", "success")
                return redirect(url_for("operation_log.detail_page", log_id=log_id))

        return render_form_page(
            page_title=CATEGORY_SETTINGS[category]["new_title"],
            submit_label="",
            form_data=form_data,
            is_new_form=True,
        )

    @blueprint.route("/technical-summaries/new", methods=["GET", "POST"])
    @login_required
    def create_technical_summary_page():
        form_data: dict[str, object] = {
            "category": "technical_summary",
            "title": "",
            "cover_image_url": "",
            "symbol": "",
            "action_summary": "",
            "content": "",
            "event_date": "",
            "published_at": "",
            "status": "draft",
        }

        if request.method == "POST":
            form_data = request.form.to_dict()
            try:
                log_id = create_log(db_path, normalize_form_payload(), session.get("username", ""))
            except ValueError as exc:
                flash(str(exc), "error")
            else:
                flash(f"{CATEGORY_LABELS['technical_summary']}已创建", "success")
                return redirect(url_for("operation_log.detail_page", log_id=log_id))

        return render_form_page(
            page_title=CATEGORY_SETTINGS["technical_summary"]["new_title"],
            submit_label="",
            form_data=form_data,
            is_new_form=True,
        )

    def _make_create_page(category_key: str):
        """Factory that returns a create-page view for a given category."""
        def view():
            form_data: dict[str, object] = {
                "category": category_key,
                "title": "",
                "cover_image_url": "",
                "symbol": "",
                "action_summary": "",
                "content": "",
                "event_date": "",
                "published_at": "",
                "status": "draft",
            }
            if request.method == "POST":
                form_data = request.form.to_dict()
                try:
                    log_id = create_log(db_path, normalize_form_payload(), session.get("username", ""))
                except ValueError as exc:
                    flash(str(exc), "error")
                else:
                    flash(f"{CATEGORY_LABELS[category_key]}已创建", "success")
                    return redirect(url_for("operation_log.detail_page", log_id=log_id))
            return render_form_page(
                page_title=CATEGORY_SETTINGS[category_key]["new_title"],
                submit_label="",
                form_data=form_data,
                is_new_form=True,
            )
        view.__name__ = f"create_{category_key}_page"
        return login_required(view)

    blueprint.add_url_rule(
        "/trading-rules/new",
        endpoint="create_trading_rule_page",
        view_func=_make_create_page("trading_rules"),
        methods=["GET", "POST"],
    )
    blueprint.add_url_rule(
        "/market-strategy/new",
        endpoint="create_market_strategy_page",
        view_func=_make_create_page("market_strategy"),
        methods=["GET", "POST"],
    )
    blueprint.add_url_rule(
        "/trend-forecast/new",
        endpoint="create_trend_forecast_page",
        view_func=_make_create_page("trend_forecast"),
        methods=["GET", "POST"],
    )

    @blueprint.post("/technical-summaries/upload-image")
    @api_login_required
    def upload_technical_summary_image():
        image = request.files.get("image")
        alt_text = request.form.get("alt_text", "").strip()
        if image is None:
            return jsonify({"error": "请选择要上传的图片"}), 400

        try:
            image_url = save_uploaded_image(image)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        alt = alt_text or Path(image.filename or "image").stem or "image"
        markdown_text = f"![{alt}]({image_url})"
        return jsonify({"url": image_url, "markdown": markdown_text})

    @blueprint.post("/technical-summaries/preview")
    @api_login_required
    def preview_technical_summary():
        content = request.form.get("content", "")
        rendered = render_markdown_document(content)
        return jsonify({"html": rendered.html, "toc_html": rendered.toc_html})

    @blueprint.get("/<int:log_id>")
    @login_required
    def detail_page(log_id: int):
        log = get_log(db_path, log_id)
        if log is None:
            abort(404)

        markdown_result = None
        if log["category"] == "technical_summary":
            markdown_result = render_markdown_document(log["content"])

        return render_template(
            "operation_log/detail.html",
            log=log,
            rendered_content=(markdown_result.html if markdown_result else None),
            rendered_toc=(markdown_result.toc_html if markdown_result else ""),
            audits=list_audits(db_path, log_id),
            page_config=CATEGORY_SETTINGS[log["category"]],
            category_labels=CATEGORY_LABELS,
            status_labels=STATUS_LABELS,
            to_datetime_local=to_datetime_local,
        )

    @blueprint.route("/<int:log_id>/edit", methods=["GET", "POST"])
    @login_required
    def edit_page(log_id: int):
        log = get_log(db_path, log_id)
        if log is None:
            abort(404)

        form_data = dict(log)
        previous_log = dict(log)
        if request.method == "POST":
            form_data = request.form.to_dict()
            try:
                updated = update_log(db_path, log_id, normalize_form_payload(log), session.get("username", ""))
            except ValueError as exc:
                flash(str(exc), "error")
            else:
                if not updated:
                    abort(404)
                cleanup_removed_images(previous_log)
                flash(f"{CATEGORY_LABELS[log['category']]}已更新", "success")
                return redirect(url_for("operation_log.detail_page", log_id=log_id))

        return render_form_page(
            page_title=CATEGORY_SETTINGS[log["category"]]["edit_title"],
            submit_label="保存修改",
            form_data=form_data,
        )

    @blueprint.post("/<int:log_id>/delete")
    @login_required
    def delete_page(log_id: int):
        existing_log = get_log(db_path, log_id)
        if existing_log is None:
            abort(404)
        deleted = delete_log(db_path, log_id, session.get("username", ""))
        if not deleted:
            abort(404)
        cleanup_removed_images(existing_log, exclude_log_id=log_id)
        flash(f"{CATEGORY_LABELS[existing_log['category']]}已删除", "success")
        return redirect(url_for(CATEGORY_SETTINGS[existing_log["category"]]["list_endpoint"]))

    return blueprint
