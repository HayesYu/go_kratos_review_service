package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	v1 "review-service/api/review/v1"
	"review-service/internal/biz"
	"review-service/internal/data/model"
	"review-service/internal/data/query"
	"review-service/pkg/snowflake"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"golang.org/x/sync/singleflight"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/go-kratos/kratos/v2/log"
)

type reviewRepo struct {
	data *Data
	log  *log.Helper
}

// NewReviewRepo .
func NewReviewRepo(data *Data, logger log.Logger) biz.ReviewRepo {
	return &reviewRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

// SaveReview 保存评价
func (r *reviewRepo) SaveReview(ctx context.Context, review *model.ReviewInfo) (*model.ReviewInfo, error) {
	err := r.data.query.ReviewInfo.WithContext(ctx).Save(review)
	return review, err
}

// GetReviewByOrderID 根据订单ID获取评价
func (r *reviewRepo) GetReviewByOrderID(ctx context.Context, orderID int64) ([]*model.ReviewInfo, error) {
	return r.data.query.ReviewInfo.WithContext(ctx).Where(r.data.query.ReviewInfo.OrderID.Eq(orderID)).Find()
}

func (r *reviewRepo) GetReview(ctx context.Context, reviewID int64) (*model.ReviewInfo, error) {
	return r.data.query.ReviewInfo.
		WithContext(ctx).
		Where(r.data.query.ReviewInfo.ReviewID.Eq(reviewID)).
		First()
}

func (r *reviewRepo) GetReviewReply(ctx context.Context, reviewID int64) (*model.ReviewReplyInfo, error) {
	return r.data.query.ReviewReplyInfo.
		WithContext(ctx).
		Where(r.data.query.ReviewReplyInfo.ReviewID.Eq(reviewID)).
		First()
}

// AuditReview 审核评价（运营对用户的评价进行审核）
func (r *reviewRepo) AuditReview(ctx context.Context, param *biz.AuditParam) error {
	_, err := r.data.query.ReviewInfo.
		WithContext(ctx).
		Where(r.data.query.ReviewInfo.ReviewID.Eq(param.ReviewID)).
		Updates(map[string]interface{}{
			"status":     param.Status,
			"op_user":    param.OpUser,
			"op_reason":  param.OpReason,
			"op_remarks": param.OpRemarks,
		})
	return err
}

// AppealReview 申诉评价（商家对用户的评价进行申诉）
func (r *reviewRepo) AppealReview(ctx context.Context, param *biz.AppealParam) (*model.ReviewAppealInfo, error) {
	// 先查询有没有申诉
	ret, err := r.data.query.ReviewAppealInfo.
		WithContext(ctx).
		Where(
			query.ReviewAppealInfo.ReviewID.Eq(param.ReviewID),
			query.ReviewAppealInfo.StoreID.Eq(param.StoreID),
		).First()
	r.log.Debugf("AppealReview query, ret:%v err:%v", ret, err)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		// 其他查询错误
		return nil, err
	}
	if err == nil && ret.Status > 10 {
		return nil, errors.New("该评价已有审核过的申诉记录")
	}
	// 查询不到审核过的申诉记录
	// 1. 有申诉记录但是处于待审核状态，需要更新
	// if ret != nil{
	// 	// update
	// }else{
	// 	// insert
	// }
	// 2. 没有申诉记录，需要创建
	appeal := &model.ReviewAppealInfo{
		ReviewID:  param.ReviewID,
		StoreID:   param.StoreID,
		Status:    10,
		Reason:    param.Reason,
		Content:   param.Content,
		PicInfo:   param.PicInfo,
		VideoInfo: param.VideoInfo,
	}
	if ret != nil {
		appeal.AppealID = ret.AppealID
	} else {
		appeal.AppealID = snowflake.GenID()
	}
	err = r.data.query.ReviewAppealInfo.
		WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "review_id"}, // ON DUPLICATE KEY
			},
			DoUpdates: clause.Assignments(map[string]interface{}{ // UPDATE
				"status":     appeal.Status,
				"content":    appeal.Content,
				"reason":     appeal.Reason,
				"pic_info":   appeal.PicInfo,
				"video_info": appeal.VideoInfo,
			}),
		}).
		Create(appeal) // INSERT
	r.log.Debugf("AppealReview, err:%v", err)
	return appeal, err
}

// AuditAppeal 审核申诉（运营对商家的申诉进行审核，审核通过会隐藏该评价）
func (r *reviewRepo) AuditAppeal(ctx context.Context, param *biz.AuditAppealParam) error {
	err := r.data.query.Transaction(func(tx *query.Query) error {
		// 申诉表
		if _, err := tx.ReviewAppealInfo.
			WithContext(ctx).
			Where(r.data.query.ReviewAppealInfo.AppealID.Eq(param.AppealID)).
			Updates(map[string]interface{}{
				"status":  param.Status,
				"op_user": param.OpUser,
			}); err != nil {
			return err
		}
		// 评价表
		if param.Status == 20 { // 申诉通过则需要隐藏评价
			if _, err := tx.ReviewInfo.WithContext(ctx).
				Where(tx.ReviewInfo.ReviewID.Eq(param.ReviewID)).
				Update(tx.ReviewInfo.Status, 40); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (r *reviewRepo) ListReviewByUserID(ctx context.Context, userID int64, offset, limit int) ([]*model.ReviewInfo, error) {
	return r.data.query.ReviewInfo.
		WithContext(ctx).
		Where(r.data.query.ReviewInfo.UserID.Eq(userID)).
		Order(r.data.query.ReviewInfo.ID.Desc()).
		Limit(limit).
		Offset(offset).
		Find()
}

func (r *reviewRepo) SaveReply(ctx context.Context, reply *model.ReviewReplyInfo) (*model.ReviewReplyInfo, error) {
	// 1.数据校验（如商家只能回复一条评论）
	review, err := r.data.query.ReviewInfo.WithContext(ctx).Where(r.data.query.ReviewInfo.ReviewID.Eq(reply.ReviewID)).First()
	if err != nil {
		return nil, err
	}
	if review.HasReply == 1 {
		return nil, v1.ErrorReplyLimit("该评价已回复，不能重复回复")
	}
	// 2.水平越权校验（A商家不能给B商家回复评价）
	if review.StoreID != reply.StoreID {
		return nil, v1.ErrorReplyLimit("商家不能回复其他商家的评价")
	}
	// 3.更新数据（评价回复表和评价表要同时更新）
	err = r.data.query.Transaction(func(tx *query.Query) error {
		// 回复表插入一条数据
		if err := tx.ReviewReplyInfo.WithContext(ctx).Save(reply); err != nil {
			r.log.WithContext(ctx).Errorf("SaveReply create reply failed: %v", err)
			return err
		}
		// 更新评价表的HasReply字段
		if _, err := tx.ReviewInfo.WithContext(ctx).Where(tx.ReviewInfo.ReviewID.Eq(reply.ReviewID)).Update(tx.ReviewInfo.HasReply, 1); err != nil {
			r.log.WithContext(ctx).Errorf("SaveReply update review has_reply failed: %v", err)
			return err
		}
		return nil
	})
	return reply, err
}

// ListReviewByStoreID 根据商家ID分页查询评价
func (r *reviewRepo) ListReviewByStoreID(ctx context.Context, storeID int64, offset, limit int) ([]*biz.MyReviewInfo, error) {
	//return r.GetDataByStoreID(ctx, storeID, offset, limit) // 第一版直接查ES
	return r.ListReviewByStoreID2(ctx, storeID, offset, limit) // 第二版带缓存的查询函数

}

func (r *reviewRepo) GetDataByStoreID(ctx context.Context, storeID int64, offset, limit int) ([]*biz.MyReviewInfo, error) {
	// 去ES中查询评价
	resp, err := r.data.es.Search().
		Index("review").
		From(offset).
		Size(limit).
		Query(&types.Query{
			Bool: &types.BoolQuery{
				Filter: []types.Query{
					{
						Term: map[string]types.TermQuery{
							"store_id": {Value: storeID},
						},
					},
				},
			},
		}).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("es result total:%v\n", resp.Hits.Total.Value)
	//b, _ := json.Marshal(resp.Hits.Hits)
	//fmt.Printf("es result hits:%s\n", b)
	list := make([]*biz.MyReviewInfo, 0, resp.Hits.Total.Value)
	// 反序列化数据
	// resp.Hits.Hits[0].Source_ (json.RawMessage) ==> model.ReviewInfo
	for _, hit := range resp.Hits.Hits {
		tmp := &biz.MyReviewInfo{}
		if err = json.Unmarshal(hit.Source_, tmp); err != nil {
			r.log.Errorf("json.Unmarshal(hit.Source_,tmp) failed, err:%v", err)
			continue
		}
		list = append(list, tmp)
	}
	return list, nil
}

var g singleflight.Group

// ListReviewByStoreID2 带缓存版本的查询函数
func (r *reviewRepo) ListReviewByStoreID2(ctx context.Context, storeID int64, offset, limit int) ([]*biz.MyReviewInfo, error) {
	// 取数据
	// 1. 先查redis
	// 2. 缓存没有则查ES
	// 3. 通过singleflight 合并短时间内大量的并发查询
	key := fmt.Sprintf("review:%d:%d:%d", storeID, offset, limit)
	b, err := r.getDataBySingleflight(ctx, key)
	if err != nil {
		return nil, err
	}
	hm := new(types.HitsMetadata)
	if err = json.Unmarshal(b, hm); err != nil {
		return nil, err
	}

	// 反序列化数据
	//b, _ := json.Marshal(resp.Hits.Hits)
	//fmt.Printf("es result hits:%s\n", b)
	list := make([]*biz.MyReviewInfo, 0, hm.Total.Value)
	// 反序列化数据
	// resp.Hits.Hits[0].Source_ (json.RawMessage) ==> model.ReviewInfo
	for _, hit := range hm.Hits {
		tmp := &biz.MyReviewInfo{}
		if err = json.Unmarshal(hit.Source_, tmp); err != nil {
			r.log.Errorf("json.Unmarshal(hit.Source_,tmp) failed, err:%v", err)
			continue
		}
		list = append(list, tmp)
	}
	return list, nil
}

// key index:storeID:offset:limit --> "[{},{},{}]"
// json.Unmarshal ([]byte)

func (r *reviewRepo) getDataBySingleflight(ctx context.Context, key string) ([]byte, error) {
	v, err, shared := g.Do(key, func() (interface{}, error) {
		// 查缓存
		data, err := r.getDataFromCache(ctx, key)
		r.log.Debugf("r.getDataFromCache(ctx, key) key: %s, data: %s, err: %v", key, data, err)
		if err == nil {
			return data, nil
		}
		// 只有在缓存中没有这个key时才会查ES
		if errors.Is(err, redis.Nil) {
			// 缓存中没有这个key，说明缓存失效了，需要查ES
			data, err = r.getDataFromES(ctx, key)
			if err == nil {
				// 查ES成功，设置缓存
				return data, r.setCache(ctx, key, data)
			}
			return nil, err
		}
		// 查缓存失败,直接返回错误，不向下传递压力
		return nil, err
	})
	r.log.Debugf("singleflight key: %s, err: %v, shared: %v", key, err, shared)
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// getDataFromCache 尝试从缓存中获取数据
func (r *reviewRepo) getDataFromCache(ctx context.Context, key string) ([]byte, error) {
	r.log.Debugf("getDataFromCache key: %s", key)
	return r.data.rdb.Get(ctx, key).Bytes()
}

// setCache 设置缓存
func (r *reviewRepo) setCache(ctx context.Context, key string, data []byte) error {
	return r.data.rdb.Set(ctx, key, data, time.Hour).Err()
}

// getDataFromES 从ES中获取数据
func (r *reviewRepo) getDataFromES(ctx context.Context, key string) ([]byte, error) {
	values := strings.Split(key, ":")
	if len(values) < 4 {
		return nil, errors.New("invalid key format, expected format: index:storeID:offset:limit")
	}
	// 去ES中查询评价
	index, storeID, offsetStr, limitStr := values[0], values[1], values[2], values[3]
	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		return nil, err
	}
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		return nil, err
	}
	resp, err := r.data.es.Search().
		Index(index).
		From(offset).
		Size(limit).
		Query(&types.Query{
			Bool: &types.BoolQuery{
				Filter: []types.Query{
					{
						Term: map[string]types.TermQuery{
							"store_id": {Value: storeID},
						},
					},
				},
			},
		}).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return json.Marshal(resp.Hits)
}
