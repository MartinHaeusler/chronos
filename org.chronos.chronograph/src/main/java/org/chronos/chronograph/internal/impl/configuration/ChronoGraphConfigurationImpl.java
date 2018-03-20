package org.chronos.chronograph.internal.impl.configuration;

import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.common.configuration.AbstractConfiguration;
import org.chronos.common.configuration.annotation.Namespace;
import org.chronos.common.configuration.annotation.Parameter;

@Namespace(ChronoGraphConfiguration.NAMESPACE)
public class ChronoGraphConfigurationImpl extends AbstractConfiguration implements ChronoGraphConfiguration {

    // =================================================================================================================
    // FIELDS
    // =================================================================================================================

    @Parameter(key = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD)
    private boolean checkIdExistenceOnAdd = true;

    @Parameter(key = ChronoGraphConfiguration.TRANSACTION_AUTO_OPEN)
    private boolean txAutoOpenEnabled = true;


    // =================================================================================================================
    // GETTERS
    // =================================================================================================================

    @Override
    public boolean isCheckIdExistenceOnAddEnabled() {
        return this.checkIdExistenceOnAdd;
    }

    @Override
    public boolean isTransactionAutoOpenEnabled() {
        return this.txAutoOpenEnabled;
    }

}
