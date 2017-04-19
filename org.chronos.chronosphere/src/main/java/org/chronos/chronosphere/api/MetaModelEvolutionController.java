package org.chronos.chronosphere.api;

import org.chronos.chronosphere.api.exceptions.MetaModelEvolutionCanceledException;

public interface MetaModelEvolutionController {

	public void migrate(MetaModelEvolutionContext context) throws MetaModelEvolutionCanceledException;

}
