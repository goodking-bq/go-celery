package celery

import (
	"context"
	"testing"
)

func TestContext(t *testing.T) {
	ctx := Context{}
	ctx.Context = context.WithValue(ctx, "zabbix", "a")
	t.Log(ctx.Value("zabbix").(string))
}
