package org.chronos.chronodb.internal.impl.query.parser.token;

import org.chronos.chronodb.internal.api.query.ChronoDBQuery;

/**
 * A {@link EndToken} is used in conjunction with an {@link BeginToken} to form matching braces for precedence handling
 * in a {@link ChronoDBQuery}.
 *
 * <p>
 * Please note that this is essentially the same as round braces in mathematical expressions. The reason that the
 * concept is called "begin" and "end" here is that Java does not allow braces for method names.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public class EndToken implements QueryToken {

}
