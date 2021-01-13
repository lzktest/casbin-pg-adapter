package hash

import "testing"

func TestMd5(t *testing.T) {
	const expectFileMd5 = "cf1cafaeb10fc123e1b68fa8613c4a86"
	actualFileMd5, err := FileMd5("../adapter.go")
	if err != nil {
		panic(err)
	}

	if expectFileMd5 != actualFileMd5 {
		t.Errorf("expect file md5 is %s; but had %s\n", expectFileMd5, actualFileMd5)
	}

	const str = "why did you like golang"
	const expectStringMd5 = "09a6f16fc1e802003b4c0c11b69761d2"
	actualStringMd5 := StringMd5(str)
	if expectStringMd5 != actualStringMd5 {
		t.Errorf("expect string md5 value is %s; but had %s\n", expectStringMd5, actualStringMd5)
	}
}

func BenchmarkMd5(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := FileMd5("./md5.go")
		if err != nil {
			panic(err)
		}
	}
	const str = "why did you like golang"
	for i := 0; i < b.N; i++ {
		_ = StringMd5(str)
	}
}