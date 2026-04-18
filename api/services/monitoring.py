"""Monitoring service: reads alerts, runs on-demand anomaly scans, and sends email alerts."""

import json
import logging
from datetime import datetime
from api.db import get_cursor
from api.config import settings

logger = logging.getLogger(__name__)

ALERT_EMAIL = "ganta.r@northeastern.edu"
NOTIFICATION_INTEGRATION = "reviewsense_email"


def send_alert_email(subject: str, body: str) -> bool:
    """Send an email alert via Snowflake SYSTEM$SEND_EMAIL."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "CALL SYSTEM$SEND_EMAIL(%s, %s, %s, %s)",
                (NOTIFICATION_INTEGRATION, ALERT_EMAIL,
                 f"[ReviewSenseAI] {subject}", body)
            )
            logger.info(f"Alert email sent: {subject}")
            return True
    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")
        return False


def get_alerts(
    severity: str | None = None,
    anomaly_type: str | None = None,
    alert_source: str | None = None,
    acknowledged: bool | None = None,
    limit: int = 50,
) -> dict:
    """Read alerts from ALERT_LOG with optional filters."""
    with get_cursor() as cur:
        conditions = []
        params = []

        if severity:
            conditions.append("SEVERITY = %s")
            params.append(severity.upper())
        if anomaly_type:
            conditions.append("ANOMALY_TYPE = %s")
            params.append(anomaly_type.upper())
        if alert_source:
            conditions.append("ALERT_SOURCE = %s")
            params.append(alert_source)
        if acknowledged is not None:
            conditions.append("ACKNOWLEDGED = %s")
            params.append(acknowledged)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        cur.execute(f"""
            SELECT ALERT_ID, ALERT_SOURCE, ANOMALY_TYPE, DERIVED_CATEGORY,
                   REVIEW_THEME, ASIN, PRODUCT_NAME, DETECTION_PERIOD,
                   CURRENT_VALUE, BASELINE_VALUE, DEVIATION_SCORE,
                   AFFECTED_REVIEWS, SEVERITY, AI_SUMMARY,
                   ACKNOWLEDGED, CREATED_AT
            FROM GOLD.ALERT_LOG
            {where}
            ORDER BY CASE SEVERITY WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 3 END,
                     ABS(COALESCE(DEVIATION_SCORE, 0)) DESC,
                     CREATED_AT DESC
            LIMIT %s
        """, params + [int(limit)])

        alerts = []
        for r in cur.fetchall():
            alerts.append({
                "alert_id": r[0],
                "alert_source": r[1],
                "anomaly_type": r[2],
                "derived_category": r[3],
                "review_theme": r[4],
                "asin": r[5],
                "product_name": r[6],
                "detection_period": str(r[7]) if r[7] else None,
                "current_value": float(r[8]) if r[8] is not None else None,
                "baseline_value": float(r[9]) if r[9] is not None else None,
                "deviation_score": float(r[10]) if r[10] is not None else None,
                "affected_reviews": r[11],
                "severity": r[12],
                "ai_summary": r[13],
                "acknowledged": r[14],
                "created_at": str(r[15]),
            })

        # Count by severity
        cur.execute("""
            SELECT SEVERITY, COUNT(*)
            FROM GOLD.ALERT_LOG
            GROUP BY SEVERITY
        """)
        counts = {r[0]: r[1] for r in cur.fetchall()}

        return {
            "alerts": alerts,
            "total": sum(counts.values()),
            "high_count": counts.get("HIGH", 0),
            "medium_count": counts.get("MEDIUM", 0),
            "low_count": counts.get("LOW", 0),
        }


def run_anomaly_scan() -> dict:
    """Run a fresh anomaly scan on-demand (same logic as dbt models, no write)."""
    with get_cursor() as cur:
        # Category anomalies
        cur.execute("""
            SELECT ANOMALY_TYPE, DERIVED_CATEGORY, REVIEW_THEME,
                   CURRENT_VALUE, BASELINE_VALUE, DEVIATION_SCORE,
                   AFFECTED_REVIEWS, SEVERITY
            FROM GOLD.REVIEW_ANOMALIES
            ORDER BY SEVERITY DESC, ABS(DEVIATION_SCORE) DESC
        """)
        anomalies = [
            {
                "anomaly_type": r[0], "derived_category": r[1],
                "review_theme": r[2], "current_value": float(r[3]) if r[3] else None,
                "baseline_value": float(r[4]) if r[4] else None,
                "deviation_score": float(r[5]) if r[5] else None,
                "affected_reviews": r[6], "severity": r[7],
            }
            for r in cur.fetchall()
        ]

        # Cross-category patterns
        cur.execute("""
            SELECT REVIEW_THEME, AFFECTED_CATEGORIES, MAX_SEVERITY,
                   TOTAL_AFFECTED_REVIEWS, AVG_DEVIATION
            FROM GOLD.CROSS_CATEGORY_ALERTS
            ORDER BY AFFECTED_CATEGORIES DESC
        """)
        cross_category = [
            {
                "review_theme": r[0], "affected_categories": r[1],
                "max_severity": r[2], "total_affected_reviews": r[3],
                "avg_deviation": float(r[4]) if r[4] else None,
            }
            for r in cur.fetchall()
        ]

        # Emerging themes
        cur.execute("""
            SELECT DERIVED_CATEGORY, REVIEW_THEME, RECENT_SHARE,
                   HISTORICAL_SHARE, GROWTH_FACTOR, SEVERITY
            FROM GOLD.EMERGING_THEMES
            ORDER BY GROWTH_FACTOR DESC
        """)
        emerging = [
            {
                "derived_category": r[0], "review_theme": r[1],
                "recent_share": float(r[2]), "historical_share": float(r[3]),
                "growth_factor": float(r[4]), "severity": r[5],
            }
            for r in cur.fetchall()
        ]

        # Product anomalies
        cur.execute("""
            SELECT ANOMALY_TYPE, ASIN, PRODUCT_NAME, BRAND, DERIVED_CATEGORY,
                   CURRENT_VALUE, BASELINE_VALUE, SEVERITY
            FROM GOLD.PRODUCT_ANOMALIES
        """)
        products = [
            {
                "anomaly_type": r[0], "asin": r[1],
                "product_name": r[2], "brand": r[3],
                "derived_category": r[4],
                "current_value": float(r[5]) if r[5] else None,
                "baseline_value": float(r[6]) if r[6] else None,
                "severity": r[7],
            }
            for r in cur.fetchall()
        ]

        # Data quality
        cur.execute("""
            SELECT CHECK_NAME, TABLE_NAME, STATUS, CURRENT_VALUE,
                   EXPECTED_VALUE, DESCRIPTION
            FROM GOLD.DATA_QUALITY_CHECKS
            ORDER BY STATUS DESC
        """)
        quality = [
            {
                "check_name": r[0], "table_name": r[1], "status": r[2],
                "current_value": float(r[3]) if r[3] is not None else None,
                "expected_value": float(r[4]) if r[4] is not None else None,
                "description": r[5],
            }
            for r in cur.fetchall()
        ]

        # Send email alert for HIGH severity anomalies
        high_anomalies = [a for a in anomalies if a["severity"] == "HIGH"]
        failed_quality = [q for q in quality if q["status"] == "FAIL"]

        if high_anomalies or failed_quality:
            alert_lines = []
            if high_anomalies:
                alert_lines.append(f"HIGH SEVERITY ANOMALIES ({len(high_anomalies)}):")
                for a in high_anomalies[:5]:
                    alert_lines.append(
                        f"  - {a['anomaly_type']} in {a['derived_category']}: "
                        f"deviation {a.get('deviation_score', 'N/A')}"
                    )
            if failed_quality:
                alert_lines.append(f"\nFAILED DATA QUALITY CHECKS ({len(failed_quality)}):")
                for q in failed_quality:
                    alert_lines.append(f"  - {q['check_name']}: {q['description']}")

            send_alert_email(
                f"Anomaly Scan: {len(high_anomalies)} HIGH alerts, {len(failed_quality)} quality failures",
                "\n".join(alert_lines)
            )

        return {
            "anomalies_detected": len(anomalies),
            "anomalies": anomalies,
            "cross_category_patterns": cross_category,
            "emerging_themes": emerging,
            "product_anomalies": products,
            "data_quality": quality,
            "email_sent": bool(high_anomalies or failed_quality),
            "generated_at": datetime.now().isoformat(),
        }


def acknowledge_alert(alert_id: str) -> bool:
    """Mark an alert as acknowledged."""
    with get_cursor() as cur:
        cur.execute("""
            UPDATE GOLD.ALERT_LOG
            SET ACKNOWLEDGED = TRUE, ACKNOWLEDGED_AT = CURRENT_TIMESTAMP()
            WHERE ALERT_ID = %s
        """, (alert_id,))
        return cur.rowcount > 0
