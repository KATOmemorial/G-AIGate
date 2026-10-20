package access

import (
	"github.com/gin-gonic/gin"

	"gaigate/internal/middleware"
)

// writeError is shared by this package's HTTP adapters. Messages must be fixed,
// public text; underlying store errors and credentials never enter the envelope.
func writeError(c *gin.Context, status int, code, message string) {
	requestID, _ := middleware.RequestIDFromContext(c.Request.Context())
	c.AbortWithStatusJSON(status, gin.H{
		"error":      gin.H{"message": message, "type": "gaigate_error", "code": code},
		"request_id": requestID,
	})
}
