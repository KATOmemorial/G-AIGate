// Package secret 实现 01-工程规范 §8 安全基线的渠道 Key 加解密：
// AES-256-GCM，master key 来自环境变量 GAIGATE_SECRET_MASTER_KEY
// （base64，解码后恰 32 字节，10.1 已校验）。密文格式为 nonce 前置拼接：
// ciphertext = nonce(12B) || AES-GCM(plaintext)。
// 契约在 13.3 §3 预留：Encrypt/Decrypt 由 access 解密渠道 Key 复用，
// 本包在 11.1 提前交付（seed 需要加密渠道 Key）。
package secret

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

// Encrypt 用 AES-256-GCM 加密 plaintext，返回 nonce 前置拼接的密文。
// masterKey 须为 32 字节（AES-256）；nonce 随机生成（01 §8）。
func Encrypt(plaintext, masterKey []byte) ([]byte, error) {
	gcm, err := newGCM(masterKey)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("secret: 生成 nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt 解密 Encrypt 产出的密文（nonce 前置拼接格式）。
// 密文被篡改或 masterKey 不符时返回错误（GCM 认证失败）。
func Decrypt(ciphertext, masterKey []byte) ([]byte, error) {
	gcm, err := newGCM(masterKey)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("secret: 密文长度不足（小于 nonce 尺寸）")
	}
	nonce, sealed := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, sealed, nil)
	if err != nil {
		return nil, fmt.Errorf("secret: 解密失败（密文损坏或密钥不符）: %w", err)
	}
	return plaintext, nil
}

func newGCM(masterKey []byte) (cipher.AEAD, error) {
	if len(masterKey) != 32 {
		return nil, fmt.Errorf("secret: masterKey 须为 32 字节（AES-256），实际 %d 字节", len(masterKey))
	}
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, fmt.Errorf("secret: 初始化 AES: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secret: 初始化 GCM: %w", err)
	}
	return gcm, nil
}
