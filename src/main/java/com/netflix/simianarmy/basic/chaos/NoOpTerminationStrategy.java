package com.netflix.simianarmy.basic.chaos;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmware.vim25.mo.VirtualMachine;

/**
 * Only logs the termination of the given VirtualMachine but DOES NOTHING else!
 *
 * @author ingmar.krusch@immobilienscout24.de
 */
public class NoOpTerminationStrategy implements TerminationStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpTerminationStrategy.class);

    @Override
    public void terminate(VirtualMachine virtualMachine) throws IOException {
        LOGGER.info("skipped termination of " + virtualMachine.getName());
    }
}
