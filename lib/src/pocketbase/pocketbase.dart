import 'dart:async';
import 'dart:collection' show SplayTreeMap;
import 'dart:convert';

import 'package:drift/drift.dart';
import 'package:flutter/material.dart';
import 'package:http/http.dart';
import 'package:http/http.dart' as http;
import 'package:logging/logging.dart';
import 'package:pocketbase_drift/pocketbase_drift.dart';

class $PocketBase extends PocketBase with WidgetsBindingObserver {
  $PocketBase(
    super.baseUrl, {
    required this.db,
    super.lang,
    super.authStore,
    super.httpClientFactory,
  }) : connectivity = ConnectivityService() {
    WidgetsBinding.instance.addObserver(this);
    _listenForConnectivityChanges();
  }

  factory $PocketBase.database(
    String baseUrl, {
    bool inMemory = false,
    String lang = "en-US",
    $AuthStore? authStore,
    DatabaseConnection? connection,
    String dbName = 'pb_drift.db',
    Client Function()? httpClientFactory,
  }) {
    final db = DataBase(
      connection ?? connect(dbName, inMemory: inMemory),
    );
    return $PocketBase(
      baseUrl,
      db: db,
      lang: lang,
      authStore: authStore?..db = db,
      httpClientFactory: httpClientFactory,
    );
  }

  final DataBase db;
  final ConnectivityService connectivity;
  final Logger logger = Logger('PocketBaseDrift.client');

  set logging(bool enable) {
    hierarchicalLoggingEnabled = true;
    logger.level = enable ? Level.ALL : Level.OFF;
  }

  StreamSubscription? _connectivitySubscription;

  Completer<void>? _syncCompleter = Completer<void>()..complete();

  Future<void> get syncCompleted => _syncCompleter?.future ?? Future.value();

  void _listenForConnectivityChanges() {
    _connectivitySubscription?.cancel();
    _connectivitySubscription = connectivity.statusStream.listen((isConnected) {
      if (isConnected) {
        logger
            .info('Connectivity restored. Retrying all pending local changes.');
        _retrySyncForAllServices();
      }
    });
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    super.didChangeAppLifecycleState(state);
    if (state == AppLifecycleState.resumed) {
      logger.info('App resumed. Resetting connectivity subscription.');
      connectivity.resetSubscription();
      if (connectivity.isConnected) {
        logger.info(
            'App resumed and online. Checking for pending changes to sync.');
        _retrySyncForAllServices();
      }
    }
  }

  Future<void> _retrySyncForAllServices() async {
    _syncCompleter = Completer<void>();

    try {
      // Query database to find services with pending records
      final pendingServicesQuery = await db
          .customSelect("SELECT DISTINCT service FROM services WHERE "
              "json_extract(data, '\$.synced') = 0 AND "
              "(json_extract(data, '\$.noSync') IS NULL OR json_extract(data, '\$.noSync') = 0)")
          .get();

      // Get all services that have ANY records (for reconciliation)
      final allServicesQuery = await db
          .customSelect("SELECT DISTINCT service FROM services WHERE service != 'schema'")
          .get();

      final allServiceNames = allServicesQuery
          .map((row) => row.read<String>('service'))
          .toSet();

      // Phase 1: Push local changes to server
      if (pendingServicesQuery.isNotEmpty) {
        logger.info('Phase 1: Pushing local changes to server...');
        final pushFutures = <Future<void>>[];

        for (final row in pendingServicesQuery) {
          final serviceName = row.read<String>('service');

          if (serviceName == 'schema') continue;

          final service = collection(serviceName);

          final future = service.retryLocal().last.then((_) {
            logger.fine('Push completed for service: $serviceName');
          });
          pushFutures.add(future);
        }

        if (pushFutures.isNotEmpty) {
          await Future.wait(pushFutures);
          logger.info('Phase 1 complete: All local changes pushed');
        }
      } else {
        logger.info('Phase 1: No pending local changes to push');
      }

      // Phase 2: Reconcile with server (pull deletions and updates)
      if (allServiceNames.isNotEmpty) {
        logger.info('Phase 2: Reconciling with server...');
        final reconcileFutures = <Future<void>>[];

        for (final serviceName in allServiceNames) {
          final service = collection(serviceName);

          final future = service.reconcileWithServer().then((_) {
            logger.fine('Reconciliation completed for service: $serviceName');
          }).catchError((e) {
            logger.warning('Reconciliation failed for $serviceName: $e');
          });
          reconcileFutures.add(future);
        }

        await Future.wait(reconcileFutures);
        logger.info('Phase 2 complete: All services reconciled with server');
      } else {
        logger.info('Phase 2: No services to reconcile');
      }

      logger.info('Full sync completed successfully');
      _syncCompleter!.complete();
    } catch (e) {
      logger.severe('Error during sync operations', e);
      if (!(_syncCompleter?.isCompleted ?? true)) {
        _syncCompleter!.completeError(e);
      }
    }
  }

  String _generateRequestCacheKey(
    String path, {
    String method = 'GET',
    Map<String, dynamic> query = const {},
    Map<String, dynamic> body = const {},
  }) {
    if (method.toUpperCase() != 'GET') {
      return '';
    }

    final sortedQuery = SplayTreeMap.from(query);
    final sortedBody = SplayTreeMap.from(body);

    return '$method::$path::${jsonEncode(sortedQuery)}::${jsonEncode(sortedBody)}';
  }

  static const _nonCacheablePathSegments = [
    'api/backups',
    'api/batch',
    'api/collections',
    'api/crons',
    'api/health',
    'api/files',
    'api/logs',
    'api/realtime',
    'api/settings',
  ];

  @override
  Future<T> send<T extends dynamic>(
    String path, {
    String method = "GET",
    Map<String, String> headers = const {},
    Map<String, dynamic> query = const {},
    Map<String, dynamic> body = const {},
    List<http.MultipartFile> files = const [],
    RequestPolicy requestPolicy = RequestPolicy.cacheAndNetwork,
  }) async {
    final cacheKey = _generateRequestCacheKey(
      path,
      method: method,
      query: query,
      body: body,
    );

    final shouldBypassCache = _nonCacheablePathSegments.any(path.contains);
    if (cacheKey.isEmpty || files.isNotEmpty || shouldBypassCache) {
      return super.send<T>(
        path,
        method: method,
        headers: headers,
        query: query,
        body: body,
        files: files,
      );
    }

    return requestPolicy.fetch<T>(
      label: 'send-$cacheKey',
      client: this,
      remote: () => super.send<T>(
        path,
        method: method,
        headers: headers,
        query: query,
        body: body,
        files: files,
      ),
      getLocal: () async {
        final cachedJson = await db.getCachedResponse(cacheKey);
        if (cachedJson == null) {
          throw Exception(
              'Response for request ($cacheKey) not found in cache.');
        }
        return jsonDecode(cachedJson) as T;
      },
      setLocal: (value) async {
        final jsonString = jsonEncode(value);
        await db.cacheResponse(cacheKey, jsonString);
      },
    );
  }

  Future<void> cacheSchema(String jsonSchema) async {
    try {
      final schema = (jsonDecode(jsonSchema) as List)
          .map((item) => item as Map<String, dynamic>)
          .toList();

      await db.setSchema(schema);
    } catch (e) {
      logger.severe('Error caching schema', e);
    }
  }

  final _recordServices = <String, $RecordService>{};

  @override
  $RecordService collection(String collectionIdOrName) {
    var service = _recordServices[collectionIdOrName];

    if (service == null) {
      service = $RecordService(this, collectionIdOrName);
      _recordServices[collectionIdOrName] = service;
    }

    return service;
  }

  Future<$RecordService> $collection(
    String collectionIdOrName, {
    RequestPolicy requestPolicy = RequestPolicy.cacheAndNetwork,
  }) async {
    await collections.getFirstListItem(
      'id = "$collectionIdOrName" || name = "$collectionIdOrName"',
      requestPolicy: requestPolicy,
    );

    var service = _recordServices[collectionIdOrName];

    if (service == null) {
      service = $RecordService(this, collectionIdOrName);
      _recordServices[collectionIdOrName] = service;
    }

    return service;
  }

  Selectable<Service> search(String query, {String? service}) {
    return db.search(query, service: service);
  }

  @override
  $CollectionService get collections => $CollectionService(this);

  @override
  $FileService get files => $FileService(this);

  @override
  void close() {
    WidgetsBinding.instance.removeObserver(this);
    _connectivitySubscription?.cancel();
    connectivity.dispose();
    super.close();
  }
}
