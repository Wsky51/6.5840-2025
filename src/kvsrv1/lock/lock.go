package lock

import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
	// "log"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	lock_state string
	identifier string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	lk.lock_state = l
	lk.identifier = kvtest.RandValue(8)
	// You may add code here
	
	return lk
}

func (lk *Lock) Acquire() {
	// log.Println("[require] lock for:",lk.identifier, ",state:",lk.lock_state)
	_, version, err := lk.ck.Get(lk.lock_state) 
	if err != rpc.OK  { // 没有加过锁
		ok := lk.ck.Put(lk.lock_state, lk.identifier, version) // 直接上锁
		if ok != rpc.OK{
			lk.Acquire() // 尝试重新获取锁
		}else{
			// log.Println("[get] lock for:",lk.identifier, ",state:",lk.lock_state, "version:", version, ",ok:",ok)
			return
		}
	}
	for {
		val, version, _ := lk.ck.Get(lk.lock_state) 
		if val == lk.identifier{
			break
		}
		if val == "unlock"{ // 有人释放锁
			ok := lk.ck.Put(lk.lock_state, lk.identifier, version) // 直接上锁
			if ok == rpc.OK{
				// log.Println("[get after] required lock for:",lk.identifier, ",state:",lk.lock_state, "version:", version, ",ok:",ok)
				break
			}
		}
	}	
}

func (lk *Lock) Release() {
	// Your code here
	
	_, version, _ := lk.ck.Get(lk.lock_state) 
	lk.ck.Put(lk.lock_state, "unlock", version) // 直接解锁
	// log.Println("[release] for:",lk.identifier, ",state:",lk.lock_state, ",version:", version,",ok:",ok, )
}
