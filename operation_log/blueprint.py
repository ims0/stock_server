from __future__ import annotations

from datetime import date, datetime
from functools import wraps

from flask import Blueprint, abort, flash, redirect, render_template, request, session, url_for

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
    parse_datetime_input,
    recent_audits_by_category,
    to_datetime_local,
    to_display_datetime,
    update_log,
)

CATEGORY_SETTINGS = {
    "operation_record": {
        "list_title": "操作记录",
        "hero_title": "操作记录复盘台",
        "hero_copy": "集中记录交易动作、复盘日期、发布时间和修改轨迹，便于后续分析与回溯。",
        "new_title": "新建操作记录",
        "edit_title": "编辑操作记录",
        "submit_label": "保存操作记录",
        "summary_label": "操作摘要",
        "summary_placeholder": "如 分批加仓 / 止盈退出 / 策略修正",
        "content_label": "复盘内容",
        "content_placeholder": "记录操作原因、执行过程、结果与后续计划",
        "event_date_label": "复盘日期",
        "symbol_label": "标的代码",
        "symbol_placeholder": "如 600519 / 00700",
        "show_event_date": True,
        "show_symbol": True,
        "list_endpoint": "operation_log.operation_records_page",
        "create_endpoint": "operation_log.create_operation_record_page",
    },
    "technical_summary": {
        "list_title": "技术总结文档",
        "hero_title": "技术总结文档台",
        "hero_copy": "集中沉淀实现方案、技术结论、问题复盘和设计经验，和交易操作记录分开管理。",
        "new_title": "新建技术总结文档",
        "edit_title": "编辑技术总结文档",
        "submit_label": "保存技术文档",
        "summary_label": "主题摘要",
        "summary_placeholder": "如 均线算法设计 / 缓存策略优化 / 页面交互总结",
        "content_label": "技术总结内容",
        "content_placeholder": "记录方案背景、实现要点、踩坑、取舍和后续优化方向",
        "event_date_label": "总结日期",
        "symbol_label": "关联标的",
        "symbol_placeholder": "可选，如 600519；无则留空",
        "show_event_date": False,
        "show_symbol": False,
        "list_endpoint": "operation_log.technical_summaries_page",
        "create_endpoint": "operation_log.create_technical_summary_page",
    },
}


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
    ensure_db(db_path)

    def login_required(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if "username" not in session:
                return redirect(url_for("login", next=request.full_path.rstrip("?")))
            return view(*args, **kwargs)

        return wrapped

    def normalize_form_payload() -> dict[str, str | None]:
        category = request.form.get("category", "operation_record").strip()
        category_config = CATEGORY_SETTINGS.get(category)
        title = request.form.get("title", "").strip()
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

        published_at = parse_datetime_input(published_at_raw) if published_at_raw.strip() else None
        if status == "published" and not published_at:
            published_at = parse_datetime_input(datetime.now().strftime("%Y-%m-%dT%H:%M"))

        if not category_config["show_symbol"]:
            symbol = ""
        if not category_config["show_event_date"]:
            if published_at:
                event_date = published_at[:10]
            else:
                event_date = date.today().isoformat()

        return {
            "category": category,
            "title": title,
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

        return render_template(
            "operation_log/list.html",
            logs=list_logs(db_path, keyword=keyword, status=status, category=category, scope=scope),
            recent_actions=recent_audits_by_category(db_path, category=category),
            stats=dashboard_stats(db_path, category=category),
            keyword=keyword,
            status=status,
            category=category,
            scope=scope,
            page_config=config,
            status_labels=STATUS_LABELS,
        )

    def render_form_page(page_title: str, submit_label: str, form_data: dict[str, object]):
        category = str(form_data.get("category") or "operation_record")
        config = CATEGORY_SETTINGS[category]
        return render_template(
            "operation_log/form.html",
            page_title=page_title,
            submit_label=submit_label,
            form_data=form_data,
            page_config=config,
            status_labels=STATUS_LABELS,
            to_datetime_local=to_datetime_local,
        )

    @blueprint.get("/")
    @login_required
    def list_logs_page():
        return redirect(url_for("operation_log.operation_records_page"))

    @blueprint.get("/records")
    @login_required
    def operation_records_page():
        return render_category_list("operation_record")

    @blueprint.get("/technical-summaries")
    @login_required
    def technical_summaries_page():
        return render_category_list("technical_summary")

    @blueprint.route("/records/new", methods=["GET", "POST"])
    @login_required
    def create_operation_record_page():
        form_data = {
            "category": "operation_record",
            "title": "",
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
                flash("操作记录已创建", "success")
                return redirect(url_for("operation_log.detail_page", log_id=log_id))

        return render_form_page(
            page_title=CATEGORY_SETTINGS["operation_record"]["new_title"],
            submit_label=CATEGORY_SETTINGS["operation_record"]["submit_label"],
            form_data=form_data,
        )

    @blueprint.route("/technical-summaries/new", methods=["GET", "POST"])
    @login_required
    def create_technical_summary_page():
        form_data = {
            "category": "technical_summary",
            "title": "",
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
                flash("技术总结文档已创建", "success")
                return redirect(url_for("operation_log.detail_page", log_id=log_id))

        return render_form_page(
            page_title=CATEGORY_SETTINGS["technical_summary"]["new_title"],
            submit_label=CATEGORY_SETTINGS["technical_summary"]["submit_label"],
            form_data=form_data,
        )

    @blueprint.get("/<int:log_id>")
    @login_required
    def detail_page(log_id: int):
        log = get_log(db_path, log_id)
        if log is None:
            abort(404)

        return render_template(
            "operation_log/detail.html",
            log=log,
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
        if request.method == "POST":
            form_data = request.form.to_dict()
            try:
                updated = update_log(db_path, log_id, normalize_form_payload(), session.get("username", ""))
            except ValueError as exc:
                flash(str(exc), "error")
            else:
                if not updated:
                    abort(404)
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
        deleted = delete_log(db_path, log_id, session.get("username", ""))
        if not deleted:
            abort(404)
        log = get_log(db_path, log_id)
        flash(f"{CATEGORY_LABELS[log['category']]}已删除", "success")
        return redirect(url_for(CATEGORY_SETTINGS[log["category"]]["list_endpoint"]))

    return blueprint