package biz

import (
	"context"
	v1 "review-service/api/review/v1"
	"review-service/internal/data/model"
	"review-service/pkg/snowflake"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

type ReviewRepo interface {
	SaveReview(context.Context, *model.ReviewInfo) (*model.ReviewInfo, error)
	GetReviewByOrderID(context.Context, int64) ([]*model.ReviewInfo, error)
	GetReview(context.Context, int64) (*model.ReviewInfo, error)
	GetReviewReply(context.Context, int64) (*model.ReviewReplyInfo, error)
	AuditReview(context.Context, *AuditParam) error
	AppealReview(context.Context, *AppealParam) (*model.ReviewAppealInfo, error)
	AuditAppeal(context.Context, *AuditAppealParam) error
	ListReviewByUserID(ctx context.Context, userID int64, offset, limit int) ([]*model.ReviewInfo, error)
	SaveReply(context.Context, *model.ReviewReplyInfo) (*model.ReviewReplyInfo, error)
	ListReviewByStoreID(ctx context.Context, storeID int64, offset, limit int) ([]*MyReviewInfo, error)
}

type ReviewUsecase struct {
	repo ReviewRepo
	log  *log.Helper
}

func NewReviewUsecase(repo ReviewRepo, logger log.Logger) *ReviewUsecase {
	return &ReviewUsecase{repo: repo, log: log.NewHelper(logger)}
}

// CreateReview 创建评价
// service 层调用该方法
func (uc *ReviewUsecase) CreateReview(ctx context.Context, review *model.ReviewInfo) (*model.ReviewInfo, error) {
	uc.log.WithContext(ctx).Debugf("[biz] CreateReview called with review: %v", review)
	// 1. 数据校验
	// 1.1 参数基础校验：正常来说不应该放在这一层（validate参数校验）
	// 1.2 业务逻辑校验：如订单之前有没有发表过评论
	reviews, err := uc.repo.GetReviewByOrderID(ctx, review.OrderID)
	if err != nil {
		return nil, v1.ErrorDbFailed("查询数据库失败")
	}
	if len(reviews) > 0 {
		return nil, v1.ErrorOrderReviewed("订单:%d已评价", review.OrderID)
	}
	// 2. 生成评价id(雪花算法，也可以用微服务)
	review.ReviewID = snowflake.GenID()
	// 3. 查询订单和商品快照信息
	// 实际业务场景下需要查询订单服务和商家服务(rpc调用订单服务和商品服务)
	// 4. 拼装数据入库
	return uc.repo.SaveReview(ctx, review)
}

// GetReview 根据评价ID获取评价
func (uc *ReviewUsecase) GetReview(ctx context.Context, reviewID int64) (*model.ReviewInfo, error) {
	uc.log.WithContext(ctx).Debugf("[biz] GetReview reviewID:%v", reviewID)
	return uc.repo.GetReview(ctx, reviewID)
}

// AuditReview 审核评价
func (uc *ReviewUsecase) AuditReview(ctx context.Context, param *AuditParam) error {
	uc.log.WithContext(ctx).Debugf("[biz] AuditReview param:%v", param)
	return uc.repo.AuditReview(ctx, param)
}

// AppealReview 申诉评价
func (uc ReviewUsecase) AppealReview(ctx context.Context, param *AppealParam) (*model.ReviewAppealInfo, error) {
	uc.log.WithContext(ctx).Debugf("[biz] AppealReview param:%v", param)
	return uc.repo.AppealReview(ctx, param)
}

// CreateReply 创建评价回复
func (uc *ReviewUsecase) CreateReply(ctx context.Context, param *ReplyParam) (*model.ReviewReplyInfo, error) {
	uc.log.WithContext(ctx).Debugf("[biz] CreateReply param:%v", param)
	return uc.repo.SaveReply(ctx, &model.ReviewReplyInfo{
		ReviewID:  param.ReviewID,
		ReplyID:   snowflake.GenID(),
		Content:   param.Content,
		StoreID:   param.StoreID,
		PicInfo:   param.PicInfo,
		VideoInfo: param.VideoInfo,
	})
}

// ListReviewByUserID 根据userID分页查询评价
func (uc ReviewUsecase) ListReviewByUserID(ctx context.Context, userID int64, page, size int) ([]*model.ReviewInfo, error) {
	if page <= 0 {
		page = 1
	}
	if size <= 0 || size > 50 {
		size = 10
	}
	offset := (page - 1) * size
	limit := size
	uc.log.WithContext(ctx).Debugf("[biz] ListReviewByUserID userID:%v", userID)
	return uc.repo.ListReviewByUserID(ctx, userID, offset, limit)
}

// AuditAppeal 审核申诉
func (uc ReviewUsecase) AuditAppeal(ctx context.Context, param *AuditAppealParam) error {
	uc.log.WithContext(ctx).Debugf("[biz] AuditAppeal param:%v", param)
	return uc.repo.AuditAppeal(ctx, param)
}

// ListReviewByStoreID 根据storeID分页查询评价
func (uc ReviewUsecase) ListReviewByStoreID(ctx context.Context, storeID int64, page, size int) ([]*MyReviewInfo, error) {
	if page <= 0 {
		page = 1
	}
	if size <= 0 || size > 50 {
		size = 10
	}
	offset := (page - 1) * size
	limit := size
	uc.log.WithContext(ctx).Debugf("[biz] ListReviewByStoreID storeID:%v", storeID)
	return uc.repo.ListReviewByStoreID(ctx, storeID, offset, limit)
}

// 放在biz层避免循环引用
// 为了防止无法将es中的时间转为go的time.Time类型，对时间进行封装
// ES中字段都是string类型，需要把int类型的字段用string类型反序列化

type MyReviewInfo struct {
	*model.ReviewInfo
	CreateAt     MyTime `json:"create_at"` // 创建时间
	UpdateAt     MyTime `json:"update_at"` // 更新时间
	ID           int64  `json:"id,string"`
	Version      int32  `json:"version,string"`
	ReviewID     int64  `json:"review_id,string"`
	Score        int32  `json:"score,string"`
	ServiceScore int32  `json:"service_score,string"`
	ExpressScore int32  `json:"express_score,string"`
	HasMedia     int32  `json:"has_media,string"` // 是否有图或视频
	OrderID      int64  `json:"order_id,string"`
	SkuID        int64  `json:"sku_id,string"`
	SpuID        int64  `json:"spu_id,string"`
	StoreID      int64  `json:"store_id,string"`
	UserID       int64  `json:"user_id,string"`
	Anonymous    int32  `json:"anonymous,string"`  // 是否匿名
	Status       int32  `json:"status,string"`     // 状态:10待审核；20审核通过；30审核不通过；40隐藏
	IsDefault    int32  `json:"is_default,string"` // 是否默认评价
	HasReply     int32  `json:"has_reply,string"`  // 是否有商家回复:0无;1有
}

type MyTime time.Time

// UnmarshalJSON 自定义时间格式化为字符串，json.Unmarshal时会自动调用该方法
func (t *MyTime) UnmarshalJSON(data []byte) error {
	// data ="\"2025-06-08 18:32:18\""
	s := strings.Trim(string(data), `"`)
	tmp, err := time.Parse(time.DateTime, s)
	if err != nil {
		return err
	}
	*t = MyTime(tmp)
	return nil
}
