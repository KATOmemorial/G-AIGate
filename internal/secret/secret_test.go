package secret

import (
	"bytes"
	"testing"
)

func testKey() []byte {
	// 32 字节测试主密钥（AES-256）
	return bytes.Repeat([]byte{0x2a}, 32)
}

func TestEncryptDecrypt_Roundtrip(t *testing.T) {
	key := testKey()
	for _, plaintext := range [][]byte{
		[]byte("sk-mock-a"),
		[]byte(""),
		bytes.Repeat([]byte("x"), 512), // VARBINARY(512) 上限场景
	} {
		ciphertext, err := Encrypt(plaintext, key)
		if err != nil {
			t.Fatalf("Encrypt(%q): %v", plaintext, err)
		}
		got, err := Decrypt(ciphertext, key)
		if err != nil {
			t.Fatalf("Decrypt: %v", err)
		}
		if !bytes.Equal(got, plaintext) {
			t.Fatalf("往返不一致: got %q want %q", got, plaintext)
		}
	}
}

func TestEncrypt_NonceRandom(t *testing.T) {
	// nonce 随机（01 §8）：同一明文两次加密产生不同密文
	key := testKey()
	c1, err := Encrypt([]byte("sk-mock-a"), key)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	c2, err := Encrypt([]byte("sk-mock-a"), key)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if bytes.Equal(c1, c2) {
		t.Fatal("两次加密密文相同：nonce 未随机化")
	}
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
	key := testKey()
	ciphertext, err := Encrypt([]byte("sk-mock-a"), key)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	ciphertext[len(ciphertext)-1] ^= 0xff
	if _, err := Decrypt(ciphertext, key); err == nil {
		t.Fatal("篡改后的密文应解密失败（GCM 认证）")
	}
}

func TestDecrypt_WrongKey(t *testing.T) {
	ciphertext, err := Encrypt([]byte("sk-mock-a"), testKey())
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	wrong := bytes.Repeat([]byte{0x00}, 32)
	if _, err := Decrypt(ciphertext, wrong); err == nil {
		t.Fatal("错误 masterKey 应解密失败")
	}
}

func TestEncrypt_KeyLength(t *testing.T) {
	for _, n := range []int{0, 16, 31, 33} {
		if _, err := Encrypt([]byte("x"), bytes.Repeat([]byte{1}, n)); err == nil {
			t.Fatalf("masterKey %d 字节应报错", n)
		}
	}
}

func TestDecrypt_ShortCiphertext(t *testing.T) {
	if _, err := Decrypt([]byte{1, 2, 3}, testKey()); err == nil {
		t.Fatal("小于 nonce 尺寸的密文应报错")
	}
}
