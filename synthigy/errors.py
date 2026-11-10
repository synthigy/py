"""SDK error model — every error the SDK raises is a SynthigyError.

Always present: .message, .code, .category, .retryable.
Optional (when the server included them): .details, .hint, .available,
.path, .entity, .relation, .operator, .request_id, .status, .line, .col,
.start, .end, .diagnostics.

Discriminate on .code (stable), never .message.
"""

# code → category. Codes not in the table fall back to 'internal', retryable
# only when the server marks the error retryable.
ERROR_CATEGORIES = {
    # auth
    "UNAUTHORIZED": "auth",
    "CLIENT_NOT_FOUND": "auth",
    "CLIENT_INACTIVE": "auth",
    "PUBLIC_CLIENT_FORBIDDEN": "auth",
    "NOT_TRUSTED": "auth",
    "USER_NOT_FOUND": "auth",
    "USER_INACTIVE": "auth",
    "PROVISION_FORBIDDEN": "auth",
    "CLAIM_INVALID": "auth",
    "NO_TOKEN": "auth",  # client-side: no token source configured — retrying can't fix it
    "NO_ENDPOINT": "validation",  # client-side: no endpoint configured — retrying can't fix it
    "IMMUTABLE_VERSION_ALTERED": "validation",
    "ENTITY_NAME_CONFLICT": "validation",
    "FIELD_KEY_CONFLICT": "validation",
    "SYSTEM_ENTITY_RLS": "validation",
    "RLS_GUARD_BROKEN": "validation",
    "VERSION_NAME_TAKEN": "validation",
    "NO_LOGIN_STORE": "auth",  # client-side: no login store configured — retrying can't fix it
    "LOGIN_NONCE_MISMATCH": "auth",
    "LOGIN_EXCHANGE_FAILED": "auth",
    "INVALID_CLIENT": "auth",
    "INVALID_SCOPE": "auth",
    "INVALID_AUDIENCE": "auth",
    # iam
    "FORBIDDEN": "iam",
    "LINKS_DISABLED": "iam",
    "FORBIDDEN_OP": "iam",
    "ENTITY_FORBIDDEN": "iam",
    "ENTITY_NOT_READABLE": "iam",
    "RELATION_NOT_READABLE": "iam",
    "RELATION_FORBIDDEN": "iam",
    "ATTRIBUTE_FORBIDDEN": "iam",
    "ROW_FORBIDDEN": "iam",
    "CREATE_FORBIDDEN": "iam",
    "DELETE_FORBIDDEN": "iam",
    "SLOT_OCCUPIED": "iam",
    "SYSTEM_DATASET_ENGINE_OWNED": "iam",
    "SYSTEM_ENTITY_ALTERED": "iam",
    "ENGINE_OWNED": "iam",
    # validation
    "INVALID_BODY": "validation",
    "NO_OPERATIONS": "validation",
    "UNKNOWN_OP": "validation",
    "UNKNOWN_OPERATOR": "validation",
    "INVALID_JOIN": "validation",
    "MISSING_ON": "validation",
    "MISSING_ROOT": "validation",
    "UNKNOWN_TREE_RELATION": "validation",
    "MISSING_RECORDS": "validation",
    "EMPTY_RECORDS": "validation",
    "MISSING_ENTITIES": "validation",
    "EMPTY_ENTITIES": "validation",
    "MISSING_RELATIONS": "validation",
    "EMPTY_RELATIONS": "validation",
    "INVALID_RELATION_NAME": "validation",
    "INVALID_SUBSCRIPTION": "validation",
    "INVALID_OPERATIONS": "validation",
    "INVALID_INTEREST": "validation",
    "EMPTY_INTEREST": "validation",
    "UNSUPPORTED_TYPE": "validation",
    "XSQL_PARSE_ERROR": "validation",
    "TEMPLATE_BAD_CTE": "validation",
    "TEMPLATE_UNBALANCED_PARENS": "validation",
    "TEMPLATE_ERROR": "validation",
    "TEMPLATE_PARAM_ERROR": "validation",
    "QUERY_NOT_SELECT": "validation",
    "PARAM_MISSING": "validation",
    "PARAM_TYPE_MISMATCH": "validation",
    "PARAM_UNKNOWN": "validation",
    "NOT_CONNECTED": "validation",
    "XID_REQUIRED": "validation",
    "NAMESPACE_REQUIRED": "validation",
    "DUPLICATE_OPERATION": "validation",
    "BATCH_MEMBER_UNRESOLVED": "validation",
    "CLAIM_METHOD_NOT_ALLOWED": "validation",
    "PASSWORD_TOO_WEAK": "validation",
    "RETURN_URL_NOT_REGISTERED": "validation",
    "LOGIN_STATE_UNKNOWN": "validation",
    "LOGIN_REQUIRES_CONFIDENTIAL_CLIENT": "validation",
    "INVALID_CONTEXT": "validation",
    "NAME_REQUIRED": "validation",
    "INVALID_NAME": "validation",
    "INVALID_ACCESS": "validation",
    "INVALID_PATH": "validation",
    "INVALID_SETTINGS": "validation",
    "INVALID_QUERY": "validation",
    "METHOD_NOT_ALLOWED": "validation",
    "INVALID_SLICE": "validation",
    "FILE_TOO_LARGE": "validation",
    "ZIP_TOO_LARGE": "validation",
    "UNSUPPORTED_MEDIA_TYPE": "validation",
    "RANGE_NOT_SATISFIABLE": "validation",
    "INVALID_EXPIRES": "validation",
    # not_found
    "UNKNOWN_ENTITY": "not_found",
    "UNKNOWN_RELATION": "not_found",
    "UNKNOWN_TEMPLATE_RELATION": "not_found",
    "HISTORY_UNAVAILABLE": "not_found",
    "NOT_FOUND": "not_found",
    "FILE_NOT_FOUND": "not_found",
    "LINK_NOT_FOUND": "not_found",
    "FOLDER_NOT_FOUND": "not_found",
    "VERSION_EXPIRED": "not_found",
    "FILES_UNAVAILABLE": "not_found",  # the files module is not running — retrying can't fix it
    "AUDIT_UNAVAILABLE": "not_found",  # file writes need audit history and none is running
    "FILES_MIGRATING": "internal",  # uploads paused while files move to a new store — retry shortly
    # conflict
    "FK_VIOLATION": "conflict",
    "MODEL_BUSY": "conflict",
    "UNIQUE_VIOLATION": "conflict",
    "CHECK_VIOLATION": "conflict",
    "NOT_NULL_VIOLATION": "conflict",
    "FILE_NAME_TAKEN": "conflict",
    "FILE_CHANGED": "conflict",
    "FILE_NOT_READY": "conflict",
    "FOLDER_NAME_TAKEN": "conflict",
    "FOLDER_NOT_EMPTY": "conflict",
    "FOLDER_CYCLE": "conflict",
    # rate / capacity
    "TIMEOUT": "rate_limit",
    # network
    "NETWORK_ERROR": "network",
    # internal — fallback
    "INTERNAL_ERROR": "internal",
    "OPERATION_ERROR": "internal",
    "HTTP_ERROR": "internal",
    "OPERATION_FAILED": "internal",
}

RETRYABLE_CATEGORIES = {"network", "rate_limit", "internal"}

# Optional structured fields copied verbatim off a server error object.
_EXTRA_FIELDS = (
    "hint", "available", "path", "entity", "relation", "operator",
    "line", "col", "start", "end", "diagnostics",
)


class SynthigyError(Exception):
    def __init__(self, message, code, details=None, *, request_id=None,
                 status=None, retryable=None, **extras):
        super().__init__(message)
        self.message = message
        self.code = code
        self.category = ERROR_CATEGORIES.get(code, "internal")
        self.retryable = (retryable if isinstance(retryable, bool)
                          else code in ERROR_CATEGORIES
                          and self.category in RETRYABLE_CATEGORIES)
        if details is not None:
            self.details = details
        if request_id is not None:
            self.request_id = request_id
        if status is not None:
            self.status = status
        for k in _EXTRA_FIELDS:
            if extras.get(k) is not None:
                setattr(self, k, extras[k])

    def __repr__(self):
        return f"SynthigyError({self.code}: {self.message!r})"


def error_from_server(err, status=None, request_id=None):
    """Build a SynthigyError from a server `{message, code, ...}` object."""
    err = err or {}
    return SynthigyError(
        err.get("message", "unknown error"),
        err.get("code", "INTERNAL_ERROR"),
        err.get("details"),
        request_id=request_id,
        status=status,
        retryable=err.get("retryable"),
        **{k: err.get(k) for k in _EXTRA_FIELDS},
    )
