"""SDK error model — every error the SDK raises is a SynthigyError.

Always present: .message, .code, .category, .retryable.
Optional (when the server included them): .details, .hint, .available,
.path, .entity, .relation, .operator, .request_id, .status, .line, .col,
.start, .end, .diagnostics.

Discriminate on .code (stable), never .message.
"""

# code → category. Codes not in the table fall back to 'internal'.
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
    # iam
    "FORBIDDEN": "iam",
    "FORBIDDEN_OP": "iam",
    "ENTITY_FORBIDDEN": "iam",
    "ENTITY_NOT_READABLE": "iam",
    "RELATION_NOT_READABLE": "iam",
    # validation
    "INVALID_BODY": "validation",
    "NO_OPERATIONS": "validation",
    "UNKNOWN_OP": "validation",
    "UNKNOWN_OPERATOR": "validation",
    "MISSING_ON": "validation",
    "MISSING_ROOT": "validation",
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
    "NOT_CONNECTED": "validation",
    "XID_REQUIRED": "validation",
    "CLAIM_METHOD_NOT_ALLOWED": "validation",
    "PASSWORD_TOO_WEAK": "validation",
    "RETURN_URL_NOT_REGISTERED": "validation",
    # not_found
    "UNKNOWN_ENTITY": "not_found",
    "UNKNOWN_RELATION": "not_found",
    "UNKNOWN_TEMPLATE_RELATION": "not_found",
    "HISTORY_UNAVAILABLE": "not_found",
    # conflict
    "FK_VIOLATION": "conflict",
    "UNIQUE_VIOLATION": "conflict",
    "CHECK_VIOLATION": "conflict",
    "NOT_NULL_VIOLATION": "conflict",
    # rate / capacity
    "TIMEOUT": "rate_limit",
    # network
    "NETWORK_ERROR": "network",
    # internal — fallback
    "INTERNAL_ERROR": "internal",
    "OPERATION_ERROR": "internal",
    "HTTP_ERROR": "internal",
}

RETRYABLE_CATEGORIES = {"network", "rate_limit", "internal"}

# Optional structured fields copied verbatim off a server error object.
_EXTRA_FIELDS = (
    "hint", "available", "path", "entity", "relation", "operator",
    "line", "col", "start", "end", "diagnostics",
)


class SynthigyError(Exception):
    def __init__(self, message, code, details=None, *, request_id=None,
                 status=None, **extras):
        super().__init__(message)
        self.message = message
        self.code = code
        self.category = ERROR_CATEGORIES.get(code, "internal")
        self.retryable = self.category in RETRYABLE_CATEGORIES
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
        **{k: err.get(k) for k in _EXTRA_FIELDS},
    )
