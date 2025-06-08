package data

import (
	"errors"
	"review-service/internal/conf"
	"review-service/internal/data/query"
	"strings"

	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewDB, NewReviewRepo, NewESClient, NewRedisClient)

// Data .
type Data struct {
	// TODO wrapped database client
	query *query.Query
	log   *log.Helper
	es    *elasticsearch.TypedClient // ES v8
	rdb   *redis.Client
}

// NewData .
func NewData(db *gorm.DB, esClient *elasticsearch.TypedClient, rdb *redis.Client, logger log.Logger) (*Data, func(), error) {
	cleanup := func() {
		log.NewHelper(logger).Info("closing the data resources")
	}
	// 非常重要!为GEN生成的query代码设置数据库对象
	query.SetDefault(db)
	return &Data{query: query.Q, log: log.NewHelper(logger), es: esClient, rdb: rdb}, cleanup, nil
}

// NewESClient 创建ES客户端
func NewESClient(cfg *conf.Elasticsearch) (*elasticsearch.TypedClient, error) {
	// ES 配置
	newCfg := elasticsearch.Config{
		Addresses: cfg.Addresses,
	}

	// 创建客户端连接
	return elasticsearch.NewTypedClient(newCfg)
}

func NewDB(c *conf.Data) (*gorm.DB, error) {
	switch strings.ToLower(c.Database.GetDriver()) {
	case "mysql":
		return gorm.Open(mysql.Open(c.Database.GetSource()))
	case "sqlite":
		return gorm.Open(sqlite.Open(c.Database.GetSource()))
	}
	return nil, errors.New("connect db fail: unsupported database driver")
}

func NewRedisClient(cfg *conf.Data) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		WriteTimeout: cfg.Redis.WriteTimeout.AsDuration(),
		ReadTimeout:  cfg.Redis.ReadTimeout.AsDuration(),
	})
}
