package org.chronos.chronosphere.impl.evolution;

import org.chronos.chronosphere.api.MetaModelEvolutionContext;
import org.chronos.chronosphere.api.MetaModelEvolutionController;
import org.chronos.chronosphere.api.MetaModelEvolutionIncubator;
import org.chronos.chronosphere.api.exceptions.ElementCannotBeEvolvedException;
import org.chronos.chronosphere.api.exceptions.MetaModelEvolutionCanceledException;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;

import java.util.Iterator;

import static com.google.common.base.Preconditions.*;

public class IncubatorBasedModelEvolutionController implements MetaModelEvolutionController {

    // =====================================================================================================================
    // FIELDS
    // =====================================================================================================================

    protected final MetaModelEvolutionIncubator incubator;

    // =====================================================================================================================
    // CONSTRUCTOR
    // =====================================================================================================================

    public IncubatorBasedModelEvolutionController(final MetaModelEvolutionIncubator incubator) {
        checkNotNull(incubator, "Precondition violation - argument 'incubator' must not be NULL!");
        this.incubator = incubator;
    }

    // =====================================================================================================================
    // PUBLIC API
    // =====================================================================================================================

    @Override
    public void migrate(final MetaModelEvolutionContext context) throws MetaModelEvolutionCanceledException {
        checkNotNull(context, "Precondition violation - argument 'context' must not be NULL!");
        // phase 1: migrate the EObjects from an old EClass to a new one
        this.executeClassEvolutionPhase(context);
        context.flush();
        // phase 2: migrate individual property values
        this.executePropertyEvolutionPhase(context);
        context.flush();
        // phase 3: migrate references
        this.executeReferenceEvolutionPhase(context);
        context.flush();
    }

    // =====================================================================================================================
    // INTERNAL HELPER METHODS
    // =====================================================================================================================

    protected void executeClassEvolutionPhase(final MetaModelEvolutionContext context) {
        // iterate over all elements from the old model
        Iterator<EObject> elementIterator = context.findInOldModel().startingFromAllEObjects().toIterator();
        while (elementIterator.hasNext()) {
            EObject oldObject = elementIterator.next();
            // ask the incubator which EClass this object should have in the new model
            try {
                EClass eClass = this.incubator.migrateClass(oldObject, context);
                if (eClass == null) {
                    // no eclass is specified... we have to discard the element
                    continue;
                }
                // create a new EObject in the new model state
                context.createAndAttachEvolvedEObject(oldObject, eClass);
            } catch (ElementCannotBeEvolvedException ignored) {
                // this element cannot be evolved; simply do not include it in the
                // new repository state
            }
        }
    }

    protected void executePropertyEvolutionPhase(final MetaModelEvolutionContext context) {
        // iterate over all elements from the new model (remember: we already transferred
        // the "plain" EObjects without contents from the old to the new model in phase 1)
        Iterator<EObject> elementIterator = context.findInNewModel().startingFromAllEObjects().toIterator();
        while (elementIterator.hasNext()) {
            EObject newObject = elementIterator.next();
            // find the corresponding old object
            EObject oldObject = context.getCorrespondingEObjectInOldModel(newObject);
            try {
                // ask the incubator to do the translation of attribute values
                this.incubator.updateAttributeValues(oldObject, newObject, context);
            } catch (ElementCannotBeEvolvedException e) {
                // the incubator realized during attribute value evolution that this
                // EObject cannot be migrated. We have to delete it from the new model,
                // because we have already persisted it during phase 1.
                context.deleteInNewModel(newObject);
            }
        }
    }

    protected void executeReferenceEvolutionPhase(final MetaModelEvolutionContext context) {
        // iterate over all elements from the new model (remember: we already transferred
        // the "plain" EObjects without contents from the old to the new model in phase 1)
        Iterator<EObject> elementIterator = context.findInNewModel().startingFromAllEObjects().toIterator();
        while (elementIterator.hasNext()) {
            EObject newObject = elementIterator.next();
            // find the corresponding old object
            EObject oldObject = context.getCorrespondingEObjectInOldModel(newObject);
            try {
                // ask the incubator to do the trnaslation of reference targets
                this.incubator.updateReferenceTargets(oldObject, newObject, context);
            } catch (ElementCannotBeEvolvedException e) {
                // the incubator realized during reference target evolution that this
                // EObject cannot be migrated. We have to delete it from the new model,
                // because we have already persisted it during phase 1 and 2.
                context.deleteInNewModel(newObject);
            }
        }
    }

}
