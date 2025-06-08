package snowflake

import (
	"errors"
	"time"

	sf "github.com/bwmarrin/snowflake"
)

var (
	InvalidInitParamErr  = errors.New("snowflake初始化失败")
	InvalidTimeFormatErr = errors.New("时间格式错误，正确格式为2006-01-02")
)

var node *sf.Node

// Init initializes the snowflake node with a start time and machine ID.
func Init(startTime string, machineID int64) (err error) {
	if len(startTime) == 0 || machineID <= 0 {
		return InvalidInitParamErr
	}
	var st time.Time
	st, err = time.Parse("2006-01-02", startTime)
	if err != nil {
		return InvalidTimeFormatErr
	}
	sf.Epoch = st.UnixNano() / 1000000
	node, err = sf.NewNode(machineID)
	return
}
func GenID() int64 {
	return node.Generate().Int64()
}
