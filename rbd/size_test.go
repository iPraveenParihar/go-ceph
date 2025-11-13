package rbd

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ceph/go-ceph/rados"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSize(t *testing.T) {
	conn := radosConnect(t)
	defer conn.Shutdown()

	poolname := GetUUID()
	err := conn.MakePool(poolname)
	assert.NoError(t, err)
	defer conn.DeletePool(poolname)

	ioctx, err := conn.OpenIOContext(poolname)
	require.NoError(t, err)
	defer ioctx.Destroy()

	t.Run("basic", func(t *testing.T) {
		testDiffIterateBasicSize(t, ioctx)
	})
}

func testDiffIterateBasicSize(t *testing.T, ioctx *rados.IOContext) {
	name := GetUUID()
	isize := uint64(1 << 30) // 1024MiB
	iorder := 22             // 4MiB
	options := NewRbdImageOptions()
	defer options.Destroy()
	assert.NoError(t, options.SetUint64(RbdImageOptionOrder, uint64(iorder)))
	err := CreateImage(ioctx, name, isize, options)
	assert.NoError(t, err)

	img, err := OpenImage(ioctx, name, NoSnapshot)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, img.Close())
		assert.NoError(t, img.Remove())
	}()

	type diResult struct {
		offset uint64
		length uint64
	}
	calls := []diResult{}

	err = img.DiffIterate(
		DiffIterateConfig{
			Offset: 0,
			Length: isize,
			Callback: func(o, l uint64, _ int, _ interface{}) int {
				calls = append(calls, diResult{offset: o, length: l})
				return 0
			},
		})
	assert.NoError(t, err)
	// Image is new, empty. Callback will not be called
	assert.Len(t, calls, 0)

	data := make([]byte, 50<<20)        // 50 MiB
	_, err = img.WriteAt(data, 0)       // write at offset 0
	_, err = img.WriteAt(data, 100<<20) // write at offset 100 MiB

	data = make([]byte, 100<<20)        // 100 MiB
	_, err = img.WriteAt(data, 600<<20) // write at offset 600 MiB
	assert.NoError(t, err)

	expectedParentLength := 209715200 // 200 MiB

	err = img.DiffIterate(
		DiffIterateConfig{
			Offset: 0,
			Length: isize,
			Callback: func(o, l uint64, _ int, _ interface{}) int {
				calls = append(calls, diResult{offset: o, length: l})
				return 0
			},
		})
	assert.NoError(t, err)

	ilen := uint64(0)
	for _, call := range calls {
		// fmt.Printf("Parent: Call %d: Offset: %d, Length: %d\n", i, call.offset, call.length)
		ilen += call.length
	}
	fmt.Printf("Parent Length: %d\n", ilen)
	assert.Equal(t, uint64(expectedParentLength), ilen)

	childImgName := "clone-image"
	tempImgName := fmt.Sprintf("%s-temp", childImgName)

	// snapshot of the parent image
	doSnapClone(t, ioctx, img, tempImgName, tempImgName)

	tempImg, err := OpenImage(ioctx, tempImgName, NoSnapshot)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, tempImg.Close())
		assert.NoError(t, tempImg.Remove())
	}()

	// snapshot of the temp image
	doSnapClone(t, ioctx, tempImg, childImgName, childImgName)

	childImg, err := OpenImage(ioctx, childImgName, NoSnapshot)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, childImg.Close())
		assert.NoError(t, childImg.Remove())
	}()

	calls = []diResult{}
	err = childImg.DiffIterate(
		DiffIterateConfig{
			Offset:        0,
			Length:        isize,
			IncludeParent: IncludeParent,
			Callback: func(o, l uint64, _ int, _ interface{}) int {
				calls = append(calls, diResult{offset: o, length: l})
				return 0
			},
		})
	assert.NoError(t, err)

	ilen = 0
	for _, call := range calls {
		// fmt.Printf("Clone: Call %d: Offset: %d, Length: %d\n", i, call.offset, call.length)
		ilen += call.length
	}
	fmt.Printf("Child Length: %d\n", ilen)
	assert.Equal(t, uint64(expectedParentLength), ilen)

	data = make([]byte, 100<<20)             // 100 MiB
	_, err = childImg.WriteAt(data, 400<<20) // write at offset 400 MiB
	assert.NoError(t, err)

	data = make([]byte, 150<<20)             // 150 MiB
	_, err = childImg.WriteAt(data, 800<<20) // write at offset 800 MiB
	assert.NoError(t, err)

	expectedChildLength := 471859200 // 450 MiB

	calls = []diResult{}
	err = childImg.DiffIterate(
		DiffIterateConfig{
			Offset:        0,
			Length:        isize,
			IncludeParent: IncludeParent,
			Callback: func(o, l uint64, _ int, _ interface{}) int {
				calls = append(calls, diResult{offset: o, length: l})
				return 0
			},
		})
	assert.NoError(t, err)

	ilen = 0
	for _, call := range calls {
		// fmt.Printf("Clone: Call %d: Offset: %d, Length: %d\n", i, call.offset, call.length)
		ilen += call.length
	}
	fmt.Printf("Child Length: %d\n", ilen)
	assert.Equal(t, uint64(expectedChildLength), ilen)

	data = make([]byte, 100<<20)             // 100 MiB
	_, err = childImg.WriteAt(data, 600<<20) // write at offset 600 MiB (overwrite)
	assert.NoError(t, err)

	calls = []diResult{}
	err = childImg.DiffIterate(
		DiffIterateConfig{
			Offset:        0,
			Length:        isize,
			IncludeParent: IncludeParent,
			Callback: func(o, l uint64, _ int, _ interface{}) int {
				calls = append(calls, diResult{offset: o, length: l})
				return 0
			},
		})
	assert.NoError(t, err)

	ilen = 0
	for _, call := range calls {
		// fmt.Printf("Clone: Call %d: Offset: %d, Length: %d\n", i, call.offset, call.length)
		ilen += call.length
	}
	fmt.Printf("Child Length: %d\n", ilen)
	assert.Equal(t, uint64(expectedChildLength), ilen)
}

func doSnapClone(t *testing.T, ioctx *rados.IOContext, img *Image, snapName, cloneImgName string) {
	snapshot, err := img.CreateSnapshot(snapName)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, snapshot.Remove())
	}()

	snapInfos, err := img.GetSnapshotNames()
	assert.NoError(t, err)
	require.Equal(t, 1, len(snapInfos))

	snapID := snapInfos[0].Id
	optionsClone := NewRbdImageOptions()
	defer optionsClone.Destroy()
	err = optionsClone.SetUint64(ImageOptionCloneFormat, 2)
	assert.NoError(t, err)

	err = CloneImageByID(ioctx, img.name, snapID, ioctx, cloneImgName, optionsClone)
	if errors.Is(err, ErrNotImplemented) {
		t.Skipf("CloneImageByID is not supported: %v", err)
	}
	assert.NoError(t, err)
}
