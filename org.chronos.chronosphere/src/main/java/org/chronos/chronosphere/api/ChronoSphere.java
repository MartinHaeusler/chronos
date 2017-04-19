package org.chronos.chronosphere.api;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.chronos.chronodb.api.ChronoDBConstants;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.api.exceptions.InvalidTransactionBranchException;
import org.chronos.chronodb.api.exceptions.InvalidTransactionTimestampException;
import org.chronos.chronosphere.emf.impl.ChronoEObjectImpl;
import org.chronos.chronosphere.emf.internal.api.ChronoEObjectInternal;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.internal.configuration.api.ChronoSphereConfiguration;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;

import com.google.common.collect.Iterators;

/**
 * This is the main repository interface, it represents the whole model repository.
 *
 * <p>
 * To get an instance of this class, please use the fluent builder API:
 *
 * <pre>
 * ChronoSphere repository = ChronoSphere.FACTORY.create() /{@literal*}... settings ... {@literal*}/ .build();
 * </pre>
 *
 * After creating this class, you should set up your {@link EPackage}s through the methods provided by
 * {@link #getEPackageManager()}. Then, you can use the various overloads of {@link #tx()} to create
 * {@linkplain ChronoSphereTransaction transactions} for working with the contents of the repository.
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface ChronoSphere extends AutoCloseable {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	/** This is the singleton factory instance for {@link ChronoSphere}s. */
	public static final ChronoSphereFactory FACTORY = ChronoSphereFactory.INSTANCE;

	// =====================================================================================================================
	// CONFIGURATION
	// =====================================================================================================================

	/**
	 * Returns the {@linkplain ChronoSphereConfiguration configuration} of this {@link ChronoSphere} instance.
	 *
	 * @return The configuration. Never <code>null</code>.
	 */
	public ChronoSphereConfiguration getConfiguration();

	// =====================================================================================================================
	// TRANSACTION MANAGEMENT
	// =====================================================================================================================

	/**
	 * Produces and returns a new instance of {@link ChronoSphereTransaction} on the <i>master</i> branch <i>head</i>
	 * version.
	 *
	 * <p>
	 * Note that transactions are in general not thread-safe and must not be shared among threads.
	 *
	 * @return The transaction. Never <code>null</code>.
	 */
	public ChronoSphereTransaction tx();

	/**
	 * Produces and returns a new instance of {@link ChronoSphereTransaction} on the <i>master</i> branch at the given
	 * timestamp.
	 *
	 * <p>
	 * Please note that implementations may choose to refuse opening a transaction on certain timestamps, e.g. when the
	 * desired timestamp is in the future. In such cases, an {@link InvalidTransactionTimestampException} is thrown.
	 *
	 * <p>
	 * Note that transactions are in general not thread-safe and must not be shared among threads.
	 *
	 * @param timestamp
	 *            The timestamp to use. Must not be negative.
	 *
	 * @return The transaction. Never <code>null</code>.
	 *
	 * @throws InvalidTransactionTimestampException
	 *             Thrown if the transaction could not be opened due to an invalid timestamp.
	 */
	public ChronoSphereTransaction tx(final long timestamp) throws InvalidTransactionTimestampException;

	/**
	 * Produces and returns a new instance of {@link ChronoSphereTransaction} on the <i>master</i> branch at the given
	 * date.
	 *
	 * <p>
	 * Please note that implementations may choose to refuse opening a transaction on certain dates, e.g. when the
	 * desired date is in the future. In such cases, an {@link InvalidTransactionTimestampException} is thrown.
	 *
	 * <p>
	 * Note that transactions are in general not thread-safe and must not be shared among threads.
	 *
	 * @param date
	 *            The date to use. Must not be negative.
	 *
	 * @return The transaction. Never <code>null</code>.
	 *
	 * @throws InvalidTransactionTimestampException
	 *             Thrown if the transaction could not be opened due to an invalid timestamp.
	 */
	public ChronoSphereTransaction tx(final Date date) throws InvalidTransactionTimestampException;

	/**
	 * Produces and returns a new instance of {@link ChronoSphereTransaction} on the <i>head</i> version of the given
	 * branch.
	 *
	 * <p>
	 * Note that transactions are in general not thread-safe and must not be shared among threads.
	 *
	 * @param branchName
	 *            The name of the branch to start a transaction on. Must not be <code>null</code>. Must be the name of
	 *            an existing branch.
	 *
	 * @return The transaction. Never <code>null</code>.
	 *
	 * @throws InvalidTransactionBranchException
	 *             Thrown if the transaction could not be opened due to an invalid branch name.
	 */
	public ChronoSphereTransaction tx(final String branchName) throws InvalidTransactionBranchException;

	/**
	 * Produces and returns a new instance of {@link ChronoSphereTransaction} at the given timestamp on the given
	 * branch.
	 *
	 * <p>
	 * Please note that implementations may choose to refuse opening a transaction on certain timestamps, e.g. when the
	 * desired timestamp is in the future. In such cases, an {@link InvalidTransactionTimestampException} is thrown.
	 *
	 * <p>
	 * Note that transactions are in general not thread-safe and must not be shared among threads.
	 *
	 * @param branchName
	 *            The name of the branch to start a transaction on. Must not be <code>null</code>. Must be the name of
	 *            an existing branch.
	 * @param timestamp
	 *            The timestamp to use. Must not be negative.
	 *
	 * @return The transaction. Never <code>null</code>.
	 *
	 * @throws InvalidTransactionBranchException
	 *             Thrown if the transaction could not be opened due to an invalid branch.
	 * @throws InvalidTransactionTimestampException
	 *             Thrown if the transaction could not be opened due to an invalid timestamp.
	 */
	public ChronoSphereTransaction tx(final String branchName, final long timestamp)
			throws InvalidTransactionBranchException, InvalidTransactionTimestampException;

	/**
	 * Produces and returns a new instance of {@link ChronoSphereTransaction} at the given date on the given branch.
	 *
	 * <p>
	 * Please note that implementations may choose to refuse opening a transaction on certain dates, e.g. when the
	 * desired date is in the future. In such cases, an {@link InvalidTransactionTimestampException} is thrown.
	 *
	 * <p>
	 * Note that transactions are in general not thread-safe and must not be shared among threads.
	 *
	 * @param branchName
	 *            The name of the branch to start a transaction on. Must not be <code>null</code>. Must be the name of
	 *            an existing branch.
	 * @param date
	 *            The date to use. Must not be negative.
	 *
	 * @return The transaction. Never <code>null</code>.
	 *
	 * @throws InvalidTransactionBranchException
	 *             Thrown if the transaction could not be opened due to an invalid branch.
	 * @throws InvalidTransactionTimestampException
	 *             Thrown if the transaction could not be opened due to an invalid timestamp.
	 */
	public ChronoSphereTransaction tx(final String branchName, final Date date)
			throws InvalidTransactionBranchException, InvalidTransactionTimestampException;

	// =====================================================================================================================
	// BULK INSERTION
	// =====================================================================================================================

	/**
	 * Batch-inserts the {@link EObject} model data from the given XMI content into the
	 * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch of this repository.
	 *
	 * <p>
	 * Please remember to {@linkplain ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage) register} your
	 * {@link EPackage}s before calling this method.
	 *
	 * <p>
	 * Only one batch insert process can be active at any point in time. Any other write transactions will be denied
	 * while this process is running.
	 *
	 * @param xmiContent
	 *            The XMI content to load. This string must contain the actual XMI contents. Must not be
	 *            <code>null</code>, must be syntactically valid XMI.
	 *
	 * @see #batchInsertModelData(File)
	 */
	public default void batchInsertModelData(final String xmiContent) {
		checkNotNull(xmiContent, "Precondition violation - argument 'xmiContent' must not be NULL!");
		this.batchInsertModelData(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, xmiContent);
	}

	/**
	 * Batch-inserts the {@link EObject} model data from the given XMI content into the given branch of this repository.
	 *
	 * <p>
	 * Please remember to {@linkplain ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage) register} your
	 * {@link EPackage}s before calling this method.
	 *
	 * <p>
	 * Only one batch insert process can be active at any point in time. Any other write transactions will be denied
	 * while this process is running.
	 *
	 * @param branch
	 *            The branch to load the model elements into. Must not be <code>null</code>, must refer to an existing
	 *            branch.
	 * @param xmiContent
	 *            The XMI content to load. This string must contain the actual XMI contents. Must not be
	 *            <code>null</code>, must be syntactically valid XMI.
	 *
	 * @see #batchInsertModelData(String, File)
	 */
	public default void batchInsertModelData(final String branch, final String xmiContent) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(this.getBranchManager().existsBranch(branch),
				"Precondition violation - argument 'branch' must refer to an existing branch!");
		checkNotNull(xmiContent, "Precondition violation - argument 'xmiContent' must not be NULL!");
		Set<EPackage> ePackages = this.getEPackageManager().getRegisteredEPackages();
		List<EObject> eObjectsFromXMI = EMFUtils.readEObjectsFromXMI(xmiContent, ePackages);
		this.batchInsertModelData(branch, eObjectsFromXMI);
	}

	/**
	 * Batch-inserts the {@link EObject} model data from the given XMI file into the
	 * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch of this repository.
	 *
	 * <p>
	 * Please remember to {@linkplain ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage) register} your
	 * {@link EPackage}s before calling this method.
	 *
	 * <p>
	 * Only one batch insert process can be active at any point in time. Any other write transactions will be denied
	 * while this process is running.
	 *
	 * @param xmiFile
	 *            The XMI file to load. Must not be <code>null</code>, must exist, must be a file, must have a file name
	 *            ending with <code>*.xmi</code>, and must contain syntactically valid XMI data.
	 */
	public default void batchInsertModelData(final File xmiFile) {
		checkNotNull(xmiFile, "Precondition violation - argument 'xmiFile' must not be NULL!");
		EMFUtils.assertIsXMIFile(xmiFile);
		this.batchInsertModelData(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, xmiFile);
	}

	/**
	 * Batch-inserts the {@link EObject} model data from the given XMI file into the given branch of this repository.
	 *
	 * <p>
	 * Please remember to {@linkplain ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage) register} your
	 * {@link EPackage}s before calling this method.
	 *
	 * <p>
	 * Only one batch insert process can be active at any point in time. Any other write transactions will be denied
	 * while this process is running.
	 *
	 * @param branch
	 *            The branch to load the model elements into. Must not be <code>null</code>, must refer to an existing
	 *            branch.
	 * @param xmiFile
	 *            The XMI file to load. Must not be <code>null</code>, must exist, must be a file, must have a file name
	 *            ending with <code>*.xmi</code>, and must contain syntactically valid XMI data.
	 */
	public default void batchInsertModelData(final String branch, final File xmiFile) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(this.getBranchManager().existsBranch(branch),
				"Precondition violation - argument 'branch' must refer to an existing branch!");
		checkNotNull(xmiFile, "Precondition violation - argument 'xmiFile' must not be NULL!");
		EMFUtils.assertIsXMIFile(xmiFile);
		ResourceSet resourceSet = EMFUtils.createResourceSet();
		for (EPackage ePackage : this.getEPackageManager().getRegisteredEPackages()) {
			resourceSet.getPackageRegistry().put(ePackage.getNsURI(), ePackage);
		}
		Resource resource = resourceSet.getResource(URI.createFileURI(xmiFile.getAbsolutePath()), true);
		this.batchInsertModelData(branch, resource.getContents());
	}

	/**
	 * Batch-inserts the given {@link EObject} model data into the
	 * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch of this repository.
	 *
	 * <p>
	 * Please remember to {@linkplain ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage) register} your
	 * {@link EPackage}s before calling this method.
	 *
	 * <p>
	 * Only one batch insert process can be active at any point in time. Any other write transactions will be denied
	 * while this process is running.
	 *
	 * @param model
	 *            The {@link EObject} model to insert into the repository. Must not be <code>null</code>. All elements
	 *            must be an instance of {@link ChronoEObjectImpl}. All {@linkplain EObject#eAllContents() contained}
	 *            EObjects will be batch-loaded, recursively.
	 */
	public default void batchInsertModelData(final EObject model) {
		checkNotNull(model, "Precondition violation - argument 'model' must not be NULL!");
		checkArgument(model instanceof ChronoEObjectInternal,
				"Precondition violation - argument 'model' is no ChronoEObject!");
		ChronoEObjectInternal chronoEObject = (ChronoEObjectInternal) model;
		checkArgument(chronoEObject.isAttached() == false,
				"Precondition violation - the given EObject is already attached to a repository!");
		this.batchInsertModelData(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, model);
	}

	/**
	 * Batch-inserts the given {@link EObject} model data into the given branch of this repository.
	 *
	 * <p>
	 * Please remember to {@linkplain ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage) register} your
	 * {@link EPackage}s before calling this method.
	 *
	 * <p>
	 * Only one batch insert process can be active at any point in time. Any other write transactions will be denied
	 * while this process is running.
	 *
	 * @param branch
	 *            The branch to load the model elements into. Must not be <code>null</code>, must refer to an existing
	 *            branch.
	 * @param model
	 *            The {@link EObject} model to insert into the repository. Must not be <code>null</code>. All elements
	 *            must be an instance of {@link ChronoEObjectImpl}. All {@linkplain EObject#eAllContents() contained}
	 *            EObjects will be batch-loaded, recursively.
	 */
	public default void batchInsertModelData(final String branch, final EObject model) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkArgument(this.getBranchManager().existsBranch(branch),
				"Precondition violation - argument 'branch' must refer to an existing branch!");
		checkNotNull(model, "Precondition violation - argument 'model' must not be NULL!");
		checkArgument(model instanceof ChronoEObjectInternal,
				"Precondition violation - argument 'model' is no ChronoEObject!");
		ChronoEObjectInternal chronoEObject = (ChronoEObjectInternal) model;
		checkArgument(chronoEObject.isAttached() == false,
				"Precondition violation - the given EObject is already attached to a repository!");
		this.batchInsertModelData(branch, Iterators.singletonIterator(model));
	}

	/**
	 * Batch-inserts the given {@link EObject} model data into the
	 * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch of this repository.
	 *
	 * <p>
	 * Please remember to {@linkplain ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage) register} your
	 * {@link EPackage}s before calling this method.
	 *
	 * <p>
	 * Only one batch insert process can be active at any point in time. Any other write transactions will be denied
	 * while this process is running.
	 *
	 * @param model
	 *            The {@link EObject} model to insert into the repository. Must not be <code>null</code>. All elements
	 *            must be an instance of {@link ChronoEObjectImpl}. All {@linkplain EObject#eAllContents() contained}
	 *            EObjects will be batch-loaded, recursively.
	 */
	public default void batchInsertModelData(final Iterable<EObject> model) {
		checkNotNull(model, "Precondition violation - argument 'model' must not be NULL!");
		this.batchInsertModelData(model.iterator());
	}

	/**
	 * Batch-inserts the given {@link EObject} model data into the given branch of this repository.
	 *
	 * <p>
	 * Please remember to {@linkplain ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage) register} your
	 * {@link EPackage}s before calling this method.
	 *
	 * <p>
	 * Only one batch insert process can be active at any point in time. Any other write transactions will be denied
	 * while this process is running.
	 *
	 * @param branch
	 *            The branch to load the model elements into. Must not be <code>null</code>, must refer to an existing
	 *            branch.
	 * @param model
	 *            The {@link EObject} model to insert into the repository. Must not be <code>null</code>. All elements
	 *            must be an instance of {@link ChronoEObjectImpl}. All {@linkplain EObject#eAllContents() contained}
	 *            EObjects will be batch-loaded, recursively.
	 */
	public default void batchInsertModelData(final String branch, final Iterable<EObject> model) {
		checkNotNull(branch, "Precondition violation - argument 'branch' must not be NULL!");
		checkNotNull(model, "Precondition violation - argument 'model' must not be NULL!");
		checkArgument(this.getBranchManager().existsBranch(branch),
				"Precondition violation - argument 'branch' does not refer to an existing branch!");
		this.batchInsertModelData(branch, model.iterator());
	}

	/**
	 * Batch-inserts the given {@link EObject} model data into the
	 * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch of this repository.
	 *
	 * <p>
	 * Please remember to {@linkplain ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage) register} your
	 * {@link EPackage}s before calling this method.
	 *
	 * <p>
	 * Only one batch insert process can be active at any point in time. Any other write transactions will be denied
	 * while this process is running.
	 *
	 * @param model
	 *            The {@link EObject} model to insert into the repository. Must not be <code>null</code>. All elements
	 *            must be an instance of {@link ChronoEObjectImpl}. All {@linkplain EObject#eAllContents() contained}
	 *            EObjects will be batch-loaded, recursively.
	 */
	public default void batchInsertModelData(final Iterator<EObject> model) {
		checkNotNull(model, "Precondition violation - argument 'model' must not be NULL!");
		this.batchInsertModelData(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, model);
	}

	/**
	 * Batch-inserts the given {@link EObject} model data into the given branch of this repository.
	 *
	 * <p>
	 * Please remember to {@linkplain ChronoSphereEPackageManager#registerOrUpdateEPackage(EPackage) register} your
	 * {@link EPackage}s before calling this method.
	 *
	 * <p>
	 * Only one batch insert process can be active at any point in time. Any other write transactions will be denied
	 * while this process is running.
	 *
	 * @param branch
	 *            The branch to load the model elements into. Must not be <code>null</code>, must refer to an existing
	 *            branch.
	 * @param model
	 *            The {@link EObject} model to insert into the repository. Must not be <code>null</code>. All elements
	 *            must be an instance of {@link ChronoEObjectImpl}. All {@linkplain EObject#eAllContents() contained}
	 *            EObjects will be batch-loaded, recursively.
	 */
	public void batchInsertModelData(String branch, Iterator<EObject> model);

	// =====================================================================================================================
	// BRANCH MANAGEMENT
	// =====================================================================================================================

	/**
	 * Returns the {@linkplain ChronoSphereBranchManager branch manager} associated with this {@link ChronoSphere}
	 * instance.
	 *
	 * @return The branch manager. Never <code>null</code>.
	 */
	public ChronoSphereBranchManager getBranchManager();

	// =====================================================================================================================
	// INDEX MANAGEMENT
	// =====================================================================================================================

	/**
	 * Returns the {@linkplain ChronoSphereIndexManager index manager} associated with the master branch of this
	 * {@link ChronoSphere} instance.
	 *
	 * @return The index manager for the master branch. Never <code>null</code>.
	 */
	public default ChronoSphereIndexManager getIndexManager() {
		return this.getIndexManager(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	/**
	 * Returns the {@linkplain ChronoSphereIndexManager index manager} associated with the given branch.
	 *
	 * @param branchName
	 *            The name of the branch to get the index manager for. Must not be <code>null</code>, must refer to an
	 *            existing branch.
	 * @return The index manager for the given branch. Never <code>null</code>.
	 */
	public ChronoSphereIndexManager getIndexManager(String branchName);

	// =================================================================================================================
	// PACKAGE MANAGEMENT
	// =================================================================================================================

	/**
	 * Returns the {@linkplain ChronoSphereEPackageManager EPackage manager} associated with this {@link ChronoSphere}
	 * instance.
	 *
	 * @return The EPackage manager. Never <code>null</code>.
	 */
	public ChronoSphereEPackageManager getEPackageManager();

	// =====================================================================================================================
	// CLOSE HANDLING
	// =====================================================================================================================

	/**
	 * Closes this {@link ChronoSphere} instance.
	 *
	 * <p>
	 * Any further attempt to work with this instance will give rise to an appropriate exception, unless noted
	 * explicitly in the method documentation.
	 *
	 * <p>
	 * This method is safe to use on closed instances. When called on an already closed instance, this method is a no-op
	 * and returns immediately.
	 */
	@Override
	public void close();

	/**
	 * Checks if this {@link ChronoSphere} instance is closed or not.
	 *
	 * <p>
	 * This method is safe to use on closed instances.
	 *
	 * @return <code>true</code> if this instance is closed, or <code>false</code> if it is still open.
	 *
	 * @see #close()
	 * @see #isOpen()
	 */
	public boolean isClosed();

	/**
	 * Checks if this {@link ChronoSphere} instance is still open.
	 *
	 * <p>
	 * This method is safe to use on closed instances.
	 *
	 * @return <code>true</code> if this instance is open, or <code>false</code> if it is already closed.
	 *
	 * @see #close()
	 * @see #isClosed()
	 */
	public default boolean isOpen() {
		return this.isClosed() == false;
	}

	/**
	 * Returns the "now" timestamp, i.e. the timestamp of the latest commit on the repository, on the master branch.
	 *
	 * <p>
	 * Requesting a transaction on this timestamp will always deliver a transaction on the "head" revision.
	 *
	 * @return The "now" timestamp. Will be zero if no commit has been taken place yet, otherwise a positive value.
	 */
	public default long getNow() {
		return this.getNow(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	/**
	 * Returns the "now" timestamp, i.e. the timestamp of the latest commit on the repository, on the given branch.
	 *
	 * <p>
	 * Requesting a transaction on this timestamp will always deliver a transaction on the "head" revision.
	 *
	 * @param branchName
	 *            The name of the branch to retrieve the "now" timestamp for. Must refer to an existing branch. Must not
	 *            be <code>null</code>.
	 *
	 * @return The "now" timestamp on the given branch. If no commits have occurred on the branch yet, this method
	 *         returns zero (master branch) or the branching timestamp (non-master branch), otherwise a positive value.
	 */
	public long getNow(String branchName);

	// =================================================================================================================
	// HISTORY ANALYSIS
	// =================================================================================================================

	/**
	 * Returns the metadata for the commit on the {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch
	 * at the given timestamp.
	 *
	 * <p>
	 * This search will include origin branches (recursively), if the timestamp is after the branching timestamp.
	 *
	 * @param timestamp
	 *            The timestamp to get the commit metadata for. Must match the commit timestamp exactly. Must not be
	 *            negative.
	 *
	 * @return The commit metadata. May be <code>null</code> if there was no metadata for the commit, or there has not
	 *         been a commit at the specified branch and timestamp.
	 */
	public default Object getCommitMetadata(final long timestamp) {
		return this.getCommitMetadata(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, timestamp);
	}

	/**
	 * Returns the metadata for the commit on the given branch at the given timestamp.
	 *
	 * <p>
	 * This search will include origin branches (recursively), if the timestamp is after the branching timestamp.
	 *
	 * @param branch
	 *            The branch to search for the commit metadata in. Must not be <code>null</code>.
	 * @param timestamp
	 *            The timestamp to get the commit metadata for. Must match the commit timestamp exactly. Must not be
	 *            negative.
	 *
	 * @return The commit metadata. May be <code>null</code> if there was no metadata for the commit, or there has not
	 *         been a commit at the specified branch and timestamp.
	 */
	public Object getCommitMetadata(String branch, long timestamp);

	/**
	 * Returns an iterator over all timestamps where commits have occurred on the
	 * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch, bounded between <code>from</code> and
	 * <code>to</code>, in descending order.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 *
	 * @return The iterator over the commit timestamps in the given time range, in descending order. May be empty, but
	 *         never <code>null</code>.
	 */
	public default Iterator<Long> getCommitTimestampsBetween(final long from, final long to) {
		return this.getCommitTimestampsBetween(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, from, to);
	}

	/**
	 * Returns an iterator over all timestamps where commits have occurred on the given branch, bounded between
	 * <code>from</code> and <code>to</code>, in descending order.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * @param branch
	 *            The name of the branch to consider. Must not be <code>null</code>, must refer to an existing branch.
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 *
	 * @return The iterator over the commit timestamps in the given time range, in descending order. May be empty, but
	 *         never <code>null</code>.
	 */
	public default Iterator<Long> getCommitTimestampsBetween(final String branch, final long from, final long to) {
		return this.getCommitTimestampsBetween(branch, from, to, Order.DESCENDING);
	}

	/**
	 * Returns an iterator over all timestamps where commits have occurred on the
	 * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch, bounded between <code>from</code> and
	 * <code>to</code>.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param order
	 *            The order of the returned timestamps. Must not be <code>null</code>.
	 *
	 * @return The iterator over the commit timestamps in the given time range. May be empty, but never
	 *         <code>null</code>.
	 */
	public default Iterator<Long> getCommitTimestampsBewteen(final long from, final long to, final Order order) {
		return this.getCommitTimestampsBetween(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, from, to, order);
	}

	/**
	 * Returns an iterator over all timestamps where commits have occurred on the
	 * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch, bounded between <code>from</code> and
	 * <code>to</code>.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param order
	 *            The order of the returned timestamps. Must not be <code>null</code>.
	 *
	 * @return The iterator over the commit timestamps in the given time range. May be empty, but never
	 *         <code>null</code>.
	 */
	public default Iterator<Long> getCommitTimestampsBetween(final long from, final long to, final Order order) {
		return this.getCommitTimestampsBetween(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, from, to, order);
	}

	/**
	 * Returns an iterator over all timestamps where commits have occurred, bounded between <code>from</code> and
	 * <code>to</code>.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * @param branch
	 *            The name of the branch to consider. Must not be <code>null</code>, must refer to an existing branch.
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param order
	 *            The order of the returned timestamps. Must not be <code>null</code>.
	 *
	 * @return The iterator over the commit timestamps in the given time range. May be empty, but never
	 *         <code>null</code>.
	 */
	public Iterator<Long> getCommitTimestampsBetween(final String branch, long from, long to, Order order);

	/**
	 * Returns an iterator over the entries of commit timestamp and associated metadata on the
	 * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch, bounded between <code>from</code> and
	 * <code>to</code>, in descending order.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * <p>
	 * Please keep in mind that some commits may not have any metadata attached. In this case, the
	 * {@linkplain Entry#getValue() value} component of the {@link Entry} will be set to <code>null</code>.
	 *
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 *
	 * @return An iterator over the commits in the given time range in descending order. The contained entries have the
	 *         timestamp as the {@linkplain Entry#getKey() key} component and the associated metadata as their
	 *         {@linkplain Entry#getValue() value} component (which may be <code>null</code>). May be empty, but never
	 *         <code>null</code>.
	 */
	public default Iterator<Entry<Long, Object>> getCommitMetadataBetween(final long from, final long to) {
		return this.getCommitMetadataBetween(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, from, to, Order.DESCENDING);
	}

	/**
	 * Returns an iterator over the entries of commit timestamp and associated metadata, bounded between
	 * <code>from</code> and <code>to</code>, in descending order.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * <p>
	 * Please keep in mind that some commits may not have any metadata attached. In this case, the
	 * {@linkplain Entry#getValue() value} component of the {@link Entry} will be set to <code>null</code>.
	 *
	 * @param branch
	 *            The name of the branch to consider. Must not be <code>null</code>, must refer to an existing branch.
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 *
	 * @return An iterator over the commits in the given time range in descending order. The contained entries have the
	 *         timestamp as the {@linkplain Entry#getKey() key} component and the associated metadata as their
	 *         {@linkplain Entry#getValue() value} component (which may be <code>null</code>). May be empty, but never
	 *         <code>null</code>.
	 */
	public default Iterator<Entry<Long, Object>> getCommitMetadataBetween(final String branch, final long from,
			final long to) {
		return this.getCommitMetadataBetween(branch, from, to, Order.DESCENDING);
	}

	/**
	 * Returns an iterator over the entries of commit timestamp and associated metadata on the
	 * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch, bounded between <code>from</code> and
	 * <code>to</code>.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * <p>
	 * Please keep in mind that some commits may not have any metadata attached. In this case, the
	 * {@linkplain Entry#getValue() value} component of the {@link Entry} will be set to <code>null</code>.
	 *
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param order
	 *            The order of the returned commits. Must not be <code>null</code>.
	 *
	 * @return An iterator over the commits in the given time range. The contained entries have the timestamp as the
	 *         {@linkplain Entry#getKey() key} component and the associated metadata as their
	 *         {@linkplain Entry#getValue() value} component (which may be <code>null</code>). May be empty, but never
	 *         <code>null</code>.
	 */
	public default Iterator<Entry<Long, Object>> getCommitMetadataBetween(final long from, final long to,
			final Order order) {
		return this.getCommitMetadataBetween(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, from, to, order);
	}

	/**
	 * Returns an iterator over the entries of commit timestamp and associated metadata, bounded between
	 * <code>from</code> and <code>to</code>.
	 *
	 * <p>
	 * If the <code>from</code> value is greater than the <code>to</code> value, this method always returns an empty
	 * iterator.
	 *
	 * <p>
	 * Please keep in mind that some commits may not have any metadata attached. In this case, the
	 * {@linkplain Entry#getValue() value} component of the {@link Entry} will be set to <code>null</code>.
	 *
	 * @param branch
	 *            The name of the branch to consider. Must not be <code>null</code>, must refer to an existing branch.
	 * @param from
	 *            The lower bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param to
	 *            The upper bound of the time range to look for commits in (inclusive). Must not be negative. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param order
	 *            The order of the returned commits. Must not be <code>null</code>.
	 *
	 * @return An iterator over the commits in the given time range. The contained entries have the timestamp as the
	 *         {@linkplain Entry#getKey() key} component and the associated metadata as their
	 *         {@linkplain Entry#getValue() value} component (which may be <code>null</code>). May be empty, but never
	 *         <code>null</code>.
	 */
	public Iterator<Entry<Long, Object>> getCommitMetadataBetween(String branch, long from, long to, Order order);

	/**
	 * Returns an iterator over commit timestamps on the {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master}
	 * branch in a paged fashion.
	 *
	 * <p>
	 * For example, calling {@code getCommitTimestampsPaged(10000, 100, 0, Order.DESCENDING)} will give the latest 100
	 * commit timestamps that have occurred before timestamp 10000. Calling
	 * {@code getCommitTimestampsPaged(123456, 200, 2, Order.DESCENDING} will return 200 commit timestamps, skipping the
	 * 400 latest commit timestamps, which are smaller than 123456.
	 *
	 * @param minTimestamp
	 *            The minimum timestamp to consider (inclusive). All lower timestamps will be excluded from the
	 *            pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param maxTimestamp
	 *            The highest timestamp to consider (inclusive). All higher timestamps will be excluded from the
	 *            pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param pageSize
	 *            The size of the page, i.e. the maximum number of elements allowed to be contained in the resulting
	 *            iterator. Must be greater than zero.
	 * @param pageIndex
	 *            The index of the page to retrieve. Must not be negative.
	 * @param order
	 *            The desired ordering for the commit timestamps
	 *
	 * @return An iterator that contains the commit timestamps for the requested page. Never <code>null</code>, may be
	 *         empty. If the requested page does not exist, this iterator will always be empty.
	 */
	public default Iterator<Long> getCommitTimestampsPaged(final long minTimestamp, final long maxTimestamp,
			final int pageSize, final int pageIndex, final Order order) {
		return this.getCommitTimestampsPaged(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, minTimestamp, maxTimestamp,
				pageSize, pageIndex, order);
	}

	/**
	 * Returns an iterator over commit timestamps in a paged fashion.
	 *
	 * <p>
	 * For example, calling {@code getCommitTimestampsPaged(10000, 100, 0, Order.DESCENDING)} will give the latest 100
	 * commit timestamps that have occurred before timestamp 10000. Calling
	 * {@code getCommitTimestampsPaged(123456, 200, 2, Order.DESCENDING} will return 200 commit timestamps, skipping the
	 * 400 latest commit timestamps, which are smaller than 123456.
	 *
	 * @param branch
	 *            The name of the branch to consider. Must not be <code>null</code>, must refer to an existing branch.
	 * @param minTimestamp
	 *            The minimum timestamp to consider (inclusive). All lower timestamps will be excluded from the
	 *            pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param maxTimestamp
	 *            The highest timestamp to consider (inclusive). All higher timestamps will be excluded from the
	 *            pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param pageSize
	 *            The size of the page, i.e. the maximum number of elements allowed to be contained in the resulting
	 *            iterator. Must be greater than zero.
	 * @param pageIndex
	 *            The index of the page to retrieve. Must not be negative.
	 * @param order
	 *            The desired ordering for the commit timestamps
	 *
	 * @return An iterator that contains the commit timestamps for the requested page. Never <code>null</code>, may be
	 *         empty. If the requested page does not exist, this iterator will always be empty.
	 */
	public Iterator<Long> getCommitTimestampsPaged(final String branch, final long minTimestamp,
			final long maxTimestamp, final int pageSize, final int pageIndex, final Order order);

	/**
	 * Returns an iterator over commit timestamps and associated metadata on the
	 * {@linkplain ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch in a paged fashion.
	 *
	 * <p>
	 * For example, calling {@code getCommitTimestampsPaged(10000, 100, 0, Order.DESCENDING)} will give the latest 100
	 * commit timestamps that have occurred before timestamp 10000. Calling
	 * {@code getCommitTimestampsPaged(123456, 200, 2, Order.DESCENDING} will return 200 commit timestamps, skipping the
	 * 400 latest commit timestamps, which are smaller than 123456.
	 *
	 * <p>
	 * The {@link Entry Entries} returned by the iterator always have the commit timestamp as their first component and
	 * the metadata associated with this commit as their second component. The second component can be <code>null</code>
	 * if the commit was executed without providing metadata.
	 *
	 * @param minTimestamp
	 *            The minimum timestamp to consider (inclusive). All lower timestamps will be excluded from the
	 *            pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param maxTimestamp
	 *            The highest timestamp to consider. All higher timestamps will be excluded from the pagination. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param pageSize
	 *            The size of the page, i.e. the maximum number of elements allowed to be contained in the resulting
	 *            iterator. Must be greater than zero.
	 * @param pageIndex
	 *            The index of the page to retrieve. Must not be negative.
	 * @param order
	 *            The desired ordering for the commit timestamps
	 *
	 * @return An iterator that contains the commits for the requested page. Never <code>null</code>, may be empty. If
	 *         the requested page does not exist, this iterator will always be empty.
	 */
	public default Iterator<Entry<Long, Object>> getCommitMetadataPaged(final long minTimestamp,
			final long maxTimestamp, final int pageSize, final int pageIndex, final Order order) {
		return this.getCommitMetadataPaged(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, minTimestamp, maxTimestamp,
				pageSize, pageIndex, order);
	}

	/**
	 * Returns an iterator over commit timestamps and associated metadata in a paged fashion.
	 *
	 * <p>
	 * For example, calling {@code getCommitTimestampsPaged(10000, 100, 0, Order.DESCENDING)} will give the latest 100
	 * commit timestamps that have occurred before timestamp 10000. Calling
	 * {@code getCommitTimestampsPaged(123456, 200, 2, Order.DESCENDING} will return 200 commit timestamps, skipping the
	 * 400 latest commit timestamps, which are smaller than 123456.
	 *
	 * <p>
	 * The {@link Entry Entries} returned by the iterator always have the commit timestamp as their first component and
	 * the metadata associated with this commit as their second component. The second component can be <code>null</code>
	 * if the commit was executed without providing metadata.
	 *
	 * @param branch
	 *            The name of the branch to consider. Must not be <code>null</code>, must refer to an existing branch.
	 * @param minTimestamp
	 *            The minimum timestamp to consider (inclusive). All lower timestamps will be excluded from the
	 *            pagination. Must be less than or equal to the timestamp of this transaction.
	 * @param maxTimestamp
	 *            The highest timestamp to consider. All higher timestamps will be excluded from the pagination. Must be
	 *            less than or equal to the timestamp of this transaction.
	 * @param pageSize
	 *            The size of the page, i.e. the maximum number of elements allowed to be contained in the resulting
	 *            iterator. Must be greater than zero.
	 * @param pageIndex
	 *            The index of the page to retrieve. Must not be negative.
	 * @param order
	 *            The desired ordering for the commit timestamps
	 *
	 * @return An iterator that contains the commits for the requested page. Never <code>null</code>, may be empty. If
	 *         the requested page does not exist, this iterator will always be empty.
	 */
	public Iterator<Entry<Long, Object>> getCommitMetadataPaged(final String branch, final long minTimestamp,
			final long maxTimestamp, final int pageSize, final int pageIndex, final Order order);

	/**
	 * Counts the number of commit timestamps on the {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master} branch
	 * between <code>from</code> (inclusive) and <code>to</code> (inclusive).
	 *
	 * <p>
	 * If <code>from</code> is greater than <code>to</code>, this method will always return zero.
	 *
	 * @param from
	 *            The minimum timestamp to include in the search (inclusive). Must not be negative. Must be less than or
	 *            equal to the timestamp of this transaction.
	 * @param to
	 *            The maximum timestamp to include in the search (inclusive). Must not be negative. Must be less than or
	 *            equal to the timestamp of this transaction.
	 *
	 * @return The number of commits that have occurred in the specified time range. May be zero, but never negative.
	 */
	public default int countCommitTimestampsBetween(final long from, final long to) {
		return this.countCommitTimestampsBetween(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER, from, to);
	}

	/**
	 * Counts the number of commit timestamps between <code>from</code> (inclusive) and <code>to</code> (inclusive).
	 *
	 * <p>
	 * If <code>from</code> is greater than <code>to</code>, this method will always return zero.
	 *
	 * @param branch
	 *            The name of the branch to consider. Must not be <code>null</code>, must refer to an existing branch.
	 * @param from
	 *            The minimum timestamp to include in the search (inclusive). Must not be negative. Must be less than or
	 *            equal to the timestamp of this transaction.
	 * @param to
	 *            The maximum timestamp to include in the search (inclusive). Must not be negative. Must be less than or
	 *            equal to the timestamp of this transaction.
	 *
	 * @return The number of commits that have occurred in the specified time range. May be zero, but never negative.
	 */
	public int countCommitTimestampsBetween(final String branch, long from, long to);

	/**
	 * Counts the total number of commit timestamps on the {@link ChronoDBConstants#MASTER_BRANCH_IDENTIFIER master}
	 * branch.
	 *
	 * @return The total number of commits in the graph.
	 */
	public default int countCommitTimestamps() {
		return this.countCommitTimestamps(ChronoDBConstants.MASTER_BRANCH_IDENTIFIER);
	}

	/**
	 * Counts the total number of commit timestamps in the graph.
	 *
	 * @param branch
	 *            The name of the branch to consider. Must not be <code>null</code>, must refer to an existing branch.
	 *
	 * @return The total number of commits in the graph.
	 */
	public int countCommitTimestamps(String branch);

}
