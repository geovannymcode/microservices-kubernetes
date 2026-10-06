package com.geovannycode.notify_service;

import com.geovannycode.notify_service.notify.infrastructure.messaging.OrderEventListener;
import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

final class ArchitectureTest {

    private static final String BASE = "com.geovannycode.notify_service.notify";
    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.geovannycode.notify_service");

    private static final DescribedPredicate<JavaMethodCall> BLOCKING_REACTOR_CALL = DescribedPredicate.describe(
            "a blocking call on Mono or Flux (block*, toIterable, toStream)",
            call -> (call.getTargetOwner().isAssignableTo(Mono.class) || call.getTargetOwner().isAssignableTo(Flux.class))
                    && (call.getName().startsWith("block") || call.getName().equals("toIterable")
                    || call.getName().equals("toStream")));

    @Test
    void apiDoesNotAccessInfrastructure() {
        noClasses().that().resideInAPackage(BASE + ".api..")
                .should().dependOnClassesThat().resideInAPackage(BASE + ".infrastructure..")
                .because("the web layer must go through the application service")
                .check(CLASSES);
    }

    @Test
    void domainDoesNotDependOnSpringOrKafka() {
        noClasses().that().resideInAPackage(BASE + ".domain..")
                .should().dependOnClassesThat().resideInAnyPackage("org.springframework..", "org.apache.kafka..")
                .because("the domain is plain Java: events, states, the channel port and its exceptions")
                .check(CLASSES);
    }

    @Test
    void onlyTheKafkaListenerBlocks() {
        noClasses().that().doNotHaveFullyQualifiedName(OrderEventListener.class.getName())
                .should().callMethodWhere(BLOCKING_REACTOR_CALL)
                .because("the Kafka consumer thread is the only place allowed to wait for a reactive chain (ADR 0007)")
                .check(CLASSES);
        // Not vacuous: the predicate does match the listener's block(Duration).
        classes().that().haveFullyQualifiedName(OrderEventListener.class.getName())
                .should().callMethodWhere(BLOCKING_REACTOR_CALL).check(CLASSES);
    }

    @Test
    void onlyTheMessagingAdapterUsesKafka() {
        noClasses().that().resideOutsideOfPackage(BASE + ".infrastructure.messaging..")
                .should().dependOnClassesThat().resideInAnyPackage("org.apache.kafka..", "org.springframework.kafka..")
                .check(CLASSES);
    }

    @Test
    void onlyTheSenderAdapterUsesWebClient() {
        noClasses().that().resideOutsideOfPackage(BASE + ".infrastructure.sender..")
                .should().dependOnClassesThat().resideInAPackage("org.springframework.web.reactive.function.client..")
                .check(CLASSES);
    }

    @Test
    void packagesAreFreeOfCycles() {
        slices().matching(BASE + ".(**)").should().beFreeOfCycles().check(CLASSES);
    }
}
