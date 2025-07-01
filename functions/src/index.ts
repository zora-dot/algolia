/* eslint-disable import/no-unresolved */
import algoliaSearch from 'algoliasearch';
import * as functions from 'firebase-functions';
import { EventContext } from 'firebase-functions';
import { DocumentData, DocumentSnapshot, getFirestore } from 'firebase-admin/firestore';
import { getExtensions } from 'firebase-admin/extensions';
import { getFunctions } from 'firebase-admin/functions';
import * as firebase from 'firebase-admin';
import { firestore } from 'firebase-admin';
import FieldPath = firestore.FieldPath;

import config from './config';
import extract from './extract';
import { areFieldsUpdated, ChangeType, getChangeType, getObjectID } from './util';
import { version } from './version';
import * as logs from './logs';

const DOCS_PER_INDEXING = 250;
const client = algoliaSearch(
  config.algoliaAppId,
  config.algoliaAPIKey,
);

client.addAlgoliaAgent('firestore_integration', version);
export const index = client.initIndex(config.algoliaIndexName);

firebase.initializeApp();
const firestoreDB = getFirestore(config.databaseId);

logs.init();

const handleCreateDocument = async (
  snapshot: DocumentSnapshot,
  timestamp: Number
) => {
  // 🔒 FILTER: Skip private profiles
  if (snapshot.get('profileVisibility') === 'private') {
    logs.info(`Skipping private profile ${snapshot.id}`);
    return;
  }

  try {
    const forceDataSync = config.forceDataSync;
    if (forceDataSync) {
      const updatedSnapshot = await snapshot.ref.get();
      const data = await extract(updatedSnapshot, 0);
      logs.createIndex(updatedSnapshot.id, data);
      logs.info('force sync data: execute saveObject');
      await index.saveObject(data);
    } else {
      const data = await extract(snapshot, timestamp);
      logs.debug({ ...data });
      logs.createIndex(snapshot.id, data);
      await index.partialUpdateObject(data, { createIfNotExists: true });
    }
  } catch (e) {
    logs.error(e);
  }
};

const handleUpdateDocument = async (
  before: DocumentSnapshot,
  after: DocumentSnapshot,
  timestamp: Number
) => {
  // 🔒 FILTER: Skip updates to private profiles
  if (after.get('profileVisibility') === 'private') {
    logs.info(`Skipping private profile update: ${after.id}`);
    return;
  }

  try {
    const forceDataSync = config.forceDataSync;
    if (forceDataSync) {
      const updatedSnapshot = await after.ref.get();
      const data = await extract(updatedSnapshot, 0);
      logs.updateIndex(updatedSnapshot.id, data);
      logs.info('force sync data: execute saveObject');
      await index.saveObject(data);
    } else {
      if (areFieldsUpdated(config, before, after)) {
        logs.debug('Detected a change, execute indexing');

        const beforeData: DocumentData = await before.data();
        const undefinedAttrs = Object.keys(beforeData).filter(key => after.get(key) === undefined || after.get(key) === null);
        logs.debug('undefinedAttrs', undefinedAttrs);

        if (undefinedAttrs.length === 0) {
          const data = await extract(after, timestamp);
          logs.updateIndex(after.id, data);
          logs.debug('execute partialUpdateObject');
          await index.partialUpdateObject(data, { createIfNotExists: true });
        } else {
          const data = await extract(after, 0);
          undefinedAttrs.forEach(attr => delete data[attr]);
          logs.updateIndex(after.id, data);
          logs.debug('execute saveObject');
          await index.saveObject(data);
        }
      }
    }
  } catch (e) {
    logs.error(e);
  }
};

const handleDeleteDocument = async (
  deleted: DocumentSnapshot,
) => {
  try {
    logs.deleteIndex(getObjectID(config, deleted));
    await index.deleteObject(getObjectID(config, deleted));
  } catch (e) {
    logs.error(e);
  }
};

export const executeIndexOperation = functions.firestore
  .document(config.collectionPath)
  .onWrite(async (change, context: EventContext): Promise<void> => {
    logs.start();

    const eventTimestamp = Date.parse(context.timestamp);
    const changeType = getChangeType(change);
    switch (changeType) {
      case ChangeType.CREATE:
        await handleCreateDocument(change.after, eventTimestamp);
        break;
      case ChangeType.DELETE:
        await handleDeleteDocument(change.before);
        break;
      case ChangeType.UPDATE:
        await handleUpdateDocument(change.before, change.after, eventTimestamp);
        break;
      default: {
        throw new Error(`Invalid change type: ${changeType}`);
      }
    }
  });

export const executeFullIndexOperation = functions.tasks
  .taskQueue()
  .onDispatch(async (data: any) => {
    const runtime = getExtensions().runtime();
    logs.init();
    logs.info('config.doFullIndexing', config.doFullIndexing);
    if (!config.doFullIndexing) {
      await runtime.setProcessingState(
        'PROCESSING_COMPLETE',
        'Existing documents were not indexed because "Indexing existing documents?" is configured to false.'
      );
      return;
    }

    logs.info('config.collectionPath', config.collectionPath);
    const docId = data['docId'] ?? null;
    const pastSuccessCount = (data['successCount'] as number) ?? 0;
    const pastErrorCount = (data['errorCount'] as number) ?? 0;
    const startTime = (data['startTime'] as number) ?? Date.now();
    let query: firebase.firestore.Query;

    const isCollectionGroup = config.collectionPath.indexOf('/') !== -1;
    if (isCollectionGroup) {
      query = firestoreDB.collectionGroup(config.collectionPath.split('/').pop());
    } else {
      query = firestoreDB.collection(config.collectionPath);
    }

    query = query.limit(DOCS_PER_INDEXING);
    if (docId) {
      const queryCursor = query.where(FieldPath.documentId(), '==', docId);
      const querySnapshot = await queryCursor.get();
      querySnapshot.docs.forEach(doc => query = query.startAfter(doc));
    }

    const snapshot = await query.get();
    const promises = await Promise.allSettled(
      snapshot.docs.map((doc) => extract(doc, startTime))
    );

    const records = (promises as any)
      .filter(v => v.status === "fulfilled" && v.value.profileVisibility !== 'private') // 🔒 FILTER: full index skip private
      .map(v => v.value);

    const responses = await index.saveObjects(records, {
      autoGenerateObjectIDIfNotExist: true,
    });

    const newSuccessCount = pastSuccessCount + records.length;
    const newErrorCount = pastErrorCount;

    if (snapshot.size === DOCS_PER_INDEXING) {
      const newCursor = snapshot.docs[snapshot.size - 1];
      const queue = getFunctions().taskQueue(
        `locations/${config.location}/functions/executeFullIndexOperation`,
        config.instanceId
      );
      await queue.enqueue({
        docId: isCollectionGroup ? newCursor.ref.path : newCursor.id,
        successCount: newSuccessCount,
        errorCount: newErrorCount,
        startTime: startTime,
      });
    } else {
      logs.fullIndexingComplete(newSuccessCount, newErrorCount);
      const message = `Successfully indexed ${newSuccessCount} documents in ${Date.now() - startTime}ms.`;
      const resultType = newErrorCount === 0 ? 'PROCESSING_COMPLETE' : newSuccessCount > 0 ? 'PROCESSING_WARNING' : 'PROCESSING_FAILED';
      await runtime.setProcessingState(resultType, message);
    }
  });
