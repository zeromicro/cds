package model

import (
	"log"
	"time"

	"github.com/tal-tech/cds/pkg/mysqlx"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
