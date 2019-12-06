package my.test.mvstore;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.junit.Test;

public class MVStoreVerstionTest {
	@Test
	public void testOpenVersion(){
		MVStore store = MVStore.open("/Users/yinguoliang/Documents/tmp/h2store/hello");
		store.setRetentionTime(100000000);
		MVMap<String, String> map = store.openMap("vtest");
		String key = "111";
//		System.out.println("currentVersion : "+store.getCurrentVersion());
//		map.put(key, "HI_v"+store.getCurrentVersion());
//		System.out.println("currentVersion : "+store.getCurrentVersion());
//		System.out.println("version " + store.getCurrentVersion() +", key="+key+",value="+map.get(key));
//		store.commit();
//		store.close();
		// 打开老的Version
//		long oldVersion = 11;
//		MVMap<String, String> old = map.openVersion(oldVersion);
//		String oldValue = old.get(key);
//		System.out.println("version " + oldVersion +", key = " + key +", value = " + oldValue);
		
		
	}
}
