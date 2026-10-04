package com.geovannycode.notify_service.notify.infrastructure.persistence;

import java.time.Duration;
import java.time.Instant;

import com.geovannycode.notify_service.TestcontainersConfiguration;
import com.geovannycode.notify_service.config.PersistenceConfiguration;
import com.geovannycode.notify_service.notify.domain.NotifyStatus;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.mongodb.test.autoconfigure.DataMongoTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@DataMongoTest
// The slice does not scan @Configuration/@Component classes: converters + auditing, the index initializer and the
// Criteria-based repository are imported explicitly.
@Import({TestcontainersConfiguration.class, PersistenceConfiguration.class, MongoIndexInitializer.class,
        NotificationQueryRepository.class})
final class NotificationRepositoryIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final NotificationRepository repository;
    private final NotificationQueryRepository queries;
    private final ReactiveMongoTemplate mongo;
    private final MongoIndexInitializer indexes;

    @Autowired
    NotificationRepositoryIT(NotificationRepository repository, NotificationQueryRepository queries,
                             ReactiveMongoTemplate mongo, MongoIndexInitializer indexes) {
        this.repository = repository;
        this.queries = queries;
        this.mongo = mongo;
        this.indexes = indexes;
    }

    @BeforeEach
    void cleanCollectionAndEnsureIndexes() {
        StepVerifier.create(repository.deleteAll().then(indexes.ready())).expectComplete().verify(TIMEOUT);
    }

    private static NotificationDocument pending(String eventId, long orderId) {
        return new NotificationDocument(null, eventId, "OrderCompleted", orderId, "AC-1550", 2, null,
                "La orden " + orderId + " fue completada.", "log", NotifyStatus.PENDING, 0, null, null, null);
    }

    @Test
    void saveAssignsIdCreatedAtAndVersion() {
        StepVerifier.create(repository.save(pending("evt-1", 1)))
                .assertNext(saved -> {
                    assertThat(saved.id()).isNotNull();
                    assertThat(saved.createdAt()).isNotNull();
                    assertThat(saved.version()).isZero();
                    assertThat(saved.status()).isEqualTo(NotifyStatus.PENDING);
                })
                .expectComplete().verify(TIMEOUT);
    }

    @Test
    void findByEventIdFindsTheDocument() {
        StepVerifier.create(repository.save(pending("evt-2", 2)).then(repository.findByEventId("evt-2")))
                .assertNext(found -> assertThat(found.orderId()).isEqualTo(2))
                .expectComplete().verify(TIMEOUT);
        StepVerifier.create(repository.findByEventId("missing")).expectComplete().verify(TIMEOUT);
    }

    @Test
    void sameEventIdTwiceIsRejectedByTheUniqueIndex() {
        StepVerifier.create(repository.save(pending("evt-dup", 3)).then(repository.save(pending("evt-dup", 3))))
                .expectError(DuplicateKeyException.class).verify(TIMEOUT);
        StepVerifier.create(repository.count()).expectNext(1L).expectComplete().verify(TIMEOUT);
    }

    @Test
    void filtersByOrderAndStatusNewestFirstWithLimit() {
        // Saved one by one so createdAt grows; order 10 has three notifications, one of them sent.
        var saves = Flux.concat(
                repository.save(pending("a", 10)),
                repository.save(pending("b", 10).markSent(Instant.now())),
                repository.save(pending("c", 20)),
                repository.save(pending("d", 10)));
        StepVerifier.create(saves.then()).expectComplete().verify(TIMEOUT);

        StepVerifier.create(queries.find(10L, null, 10).map(NotificationDocument::eventId).collectList())
                .assertNext(ids -> assertThat(ids).containsExactly("d", "b", "a")).expectComplete().verify(TIMEOUT);
        StepVerifier.create(queries.find(10L, NotifyStatus.PENDING, 10).map(NotificationDocument::eventId).collectList())
                .assertNext(ids -> assertThat(ids).containsExactly("d", "a")).expectComplete().verify(TIMEOUT);
        StepVerifier.create(queries.find(null, NotifyStatus.SENT, 10).map(NotificationDocument::eventId).collectList())
                .assertNext(ids -> assertThat(ids).containsExactly("b")).expectComplete().verify(TIMEOUT);
        StepVerifier.create(queries.find(null, null, 2).map(NotificationDocument::eventId).collectList())
                .assertNext(ids -> assertThat(ids).containsExactly("d", "c")).expectComplete().verify(TIMEOUT);
    }

    @Test
    void statusIsStoredInLowercase() {
        StepVerifier.create(repository.save(pending("evt-raw", 4).markFailed())
                        .flatMap(saved -> mongo.findById(saved.id(), Document.class, MongoIndexInitializer.COLLECTION)))
                .assertNext(raw -> assertThat(raw.getString("status")).isEqualTo("failed"))
                .expectComplete().verify(TIMEOUT);
    }

    @Test
    void secondWriteOnTheSameVersionFailsWithOptimisticLocking() {
        StepVerifier.create(repository.save(pending("evt-lock", 5))
                        .flatMap(stale -> repository.save(stale.withAttempt()).then(repository.save(stale.markFailed()))))
                .expectError(OptimisticLockingFailureException.class).verify(TIMEOUT);
    }

    @Test
    void indexesExistAndCreatingThemAgainIsIdempotent() {
        StepVerifier.create(new MongoIndexInitializer(mongo).ready()
                        .thenMany(mongo.indexOps(MongoIndexInitializer.COLLECTION).getIndexInfo())
                        .map(index -> index.getName()).collectList())
                .assertNext(names -> assertThat(names).contains("ux_event_id", "ix_order_id", "ix_status_created_at"))
                .expectComplete().verify(TIMEOUT);
        StepVerifier.create(mongo.indexOps(MongoIndexInitializer.COLLECTION).getIndexInfo()
                        .filter(index -> index.getName().equals("ux_event_id")))
                .assertNext(index -> assertThat(index.isUnique()).isTrue())
                .expectComplete().verify(TIMEOUT);
    }
}
