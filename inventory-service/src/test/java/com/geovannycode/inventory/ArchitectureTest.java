package com.geovannycode.inventory;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

final class ArchitectureTest {

    private static final String BASE = "com.geovannycode.inventory.inventory";
    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.geovannycode.inventory");

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
    void packagesAreFreeOfCycles() {
        // One slice per package: api.dto is the shared contract model, so application -> api.dto is not a cycle.
        slices().matching("com.geovannycode.inventory.(**)").should().beFreeOfCycles().check(CLASSES);
    }
}
