package com.geovannycode.order;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

final class ArchitectureTest {

    private static final String BASE = "com.geovannycode.order.order";
    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.geovannycode.order");

    @Test
    void apiDoesNotAccessInfrastructure() {
        noClasses().that().resideInAPackage(BASE + ".api..")
                .should().dependOnClassesThat().resideInAPackage(BASE + ".infrastructure..")
                .because("the web layer must go through the application service")
                .check(CLASSES);
    }

    @Test
    void domainDoesNotDependOnSpring() {
        noClasses().that().resideInAPackage(BASE + ".domain..")
                .should().dependOnClassesThat().resideInAPackage("org.springframework..")
                .check(CLASSES);
    }

    @Test
    void onlyTheInventoryAdapterUsesWebClientAndResilience4j() {
        noClasses().that().resideOutsideOfPackages(BASE + ".infrastructure.inventory..", "com.geovannycode.order.generated..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.springframework.web.reactive.function.client..", "org.springframework.web.service..",
                        "io.github.resilience4j..")
                .because("HTTP and resilience details toward Inventory stay behind InventoryGateway")
                .check(CLASSES);
    }

    @Test
    void packagesAreFreeOfCycles() {
        slices().matching("com.geovannycode.order.order.(**)").should().beFreeOfCycles().check(CLASSES);
    }
}
