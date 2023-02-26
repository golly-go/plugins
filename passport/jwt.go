package passport

import (
	"fmt"
	"os"
	"regexp"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/google/uuid"
	"github.com/slimloans/golly"
	"github.com/slimloans/golly/errors"
)

var (
	usersHeaderMatcher = regexp.MustCompile(`[bB]earer\s(.+)`)

	// ErrorExpiredClaim jwt token is expired
	ErrorExpiredClaim = fmt.Errorf("jwt token is expired")

	// ErrorInvalidOrNoUser no user id is present for the identity
	ErrorInvalidOrNoUser = fmt.Errorf("invalid or no user")

	ErrorInvalidSource = fmt.Errorf("invalid source for token")

	ErrorInvalidClaim = fmt.Errorf("invalid claim for token")
)

// Identity holds the JWT identity of a user
type JWT struct {
	jwt.StandardClaims
}

// JWT - jwtEncode
func (ident JWT) Token(identity Identity) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, identity)
	return token.SignedString(secret())
}

func secret() []byte {
	if s := os.Getenv("JWT_SECRET"); s != "" {
		return []byte(s)
	}
	return []byte(golly.Secret())
}

// Issue issues a new ident
func (ident JWT) Issue() JWT {
	t := time.Now()

	ident.IssuedAt = t.Unix()
	ident.ExpiresAt = t.Add(time.Hour * 1).Unix()
	ident.NotBefore = ident.IssuedAt

	return ident
}

// Valid returns error if not valid
func (ident JWT) Valid() error {
	if !ident.StandardClaims.VerifyExpiresAt(time.Now().Unix(), true) {
		return errors.WrapForbidden(ErrorExpiredClaim)
	}

	if !ident.StandardClaims.VerifyNotBefore(time.Now().Unix(), true) {
		return errors.WrapForbidden(ErrorInvalidClaim)
	}

	return nil
}

// IdentityFromUser returns an identity object from a user
func NewJWT(subject string) JWT {
	id, _ := uuid.NewRandom()
	return JWT{
		StandardClaims: jwt.StandardClaims{
			Id:      id.String(),
			Subject: subject,
		},
	}
}
