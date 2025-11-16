package io.synapsedb.synapsedbserver.controller;

import io.synapsedb.core.SynapseDB;
import io.synapsedb.core.manager.SynapseDbManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/database")
public class DatabaseController {

    private final SynapseDbManager dbManager;

    public DatabaseController(SynapseDbManager dbManager) {
        this.dbManager = dbManager;
    }






}
