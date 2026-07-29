package com.dev.cache.expiry;

import com.dev.cache.store.StorageEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ExpiryService {

    private  final StorageEngine storage;

    public ExpiryService(StorageEngine storage){
        this.storage=storage;
    }

    @Scheduled(fixedDelayString = "${cache.expiry.sweep-period-ms:1000}")
    public void sweep(){
        int removed = storage.sweepExpired();
        if(removed>0){
            log.debug("Expiry sweep removed {} entries", removed);
        }
    }
}
