package model

import (
	"log"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"encoding/base64"
	"github.com/tal-tech/cds/tools/mysqlx"
)

type (
	User struct {
		ID         int       `db:"id"`
		GroupID    int       `db:"group_id" index:""`
		Name       string    `db:"name" length:"36"`
		Email      string    `db:"email" length:"36" index:""`
		Password   string    `db:"password" length:"36"`
		Token      string    `db:"token" length:"36" index:""`
		CreateTime time.Time `db:"create_time"`
		UpdateTime time.Time `db:"update_time"`
	}
	UserModel struct {
		base *mysqlx.MySQLModel
	}
)

func NewUserModel(dsn string) *UserModel {
	u := &UserModel{}
	var e error
	u.base, _, e = mysqlx.NewMySQLModel("", dsn, User{})
	if e != nil {
		log.Fatal(e)
	}
	return u
}

func (u *UserModel) FindByEmail(email string) (*User, error) {
	v, e := u.base.FindBy("email", email)
	if e != nil {
		return nil, e
	}
	return v.(*User), nil
}

func (u *UserModel) Register(user User) (string, error) {
	user.Token = primitive.NewObjectID().Hex()
	_, e := u.base.Insert(user)
	if e != nil {
		return "", e
	}
	return user.Token, nil
}

func (u *UserModel) UpdateToken(uid int) (string, error) {
	timeString := strconv.FormatInt(time.Now().Unix(), 10)
	token := base64.StdEncoding.EncodeToString([]byte(timeString))
	if _, err := u.base.Update(uid, "token=?", token); err != nil {
		return "", err
	}
	return token, nil
}

func (u *UserModel) FindByToken(token string) (*User, error) {
	v, e := u.base.FindBy("token", token)
	if e != nil {
		return nil, e
	}
	return v.(*User), nil
}
