import json
import boto3
import urllib.parse

sf = boto3.client("stepfunctions")

def _response(status_code: int, body):
    if isinstance(body, dict):
        body = json.dumps(body)
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": body,
    }

def lambda_handler(event, context):
    path = (event.get("rawPath") or event.get("path") or "").lower()
    qsp = event.get("queryStringParameters") or {}

    if not qsp and event.get("rawQueryString"):
        qsp = dict(urllib.parse.parse_qsl(event["rawQueryString"]))

    task_token = qsp.get("taskToken") or qsp.get("token")
    comment = qsp.get("comment", "")

    if not task_token:
        return _response(400, {"message": "Missing required query parameter: taskToken"})

    if "/approve" in path:
        sf.send_task_success(
            taskToken=task_token,
            output=json.dumps({"approved": True, "comment": comment}),
        )
        return _response(200, {"status": "approved", "message": "Approved. You can close this tab."})

    if "/reject" in path:
        sf.send_task_failure(
            taskToken=task_token,
            error="RejectedBySteward",
            cause=comment or "Rejected",
        )
        return _response(200, {"status": "rejected", "message": "Rejected. You can close this tab."})

    return _response(404, {"message": "Unknown route. Use /approve or /reject."})
